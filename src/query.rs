use hyper::{
    Method, Request,
    header::{CONTENT_LENGTH, CONTENT_TYPE},
};
use serde::Serialize;
use std::fmt::Display;
use tracing::Instrument;
use url::Url;

use crate::{
    Client,
    error::{Error, Result},
    external_data::{self, ExternalTable},
    formats,
    headers::with_request_headers,
    request_body::RequestBody,
    response::Response,
    row::{Row, RowOwned, RowRead},
    sql::{Bind, SqlBuilder, ser},
};

pub use crate::cursors::{BytesCursor, RowCursor};
use crate::headers::with_authentication;
use crate::settings;

#[must_use]
#[derive(Clone)]
pub struct Query {
    client: Client,
    sql: SqlBuilder,
    external_tables: Vec<ExternalTable>,
}

impl Query {
    pub(crate) fn new(client: &Client, template: &str) -> Self {
        Self {
            client: client.clone(),
            sql: SqlBuilder::new(template),
            external_tables: vec![],
        }
    }

    /// Display SQL query as string.
    pub fn sql_display(&self) -> &impl Display {
        &self.sql
    }

    /// Binds `value` to the next `?` in the query.
    ///
    /// The `value`, which must either implement [`Serialize`] or be an
    /// [`Identifier`], will be appropriately escaped.
    ///
    /// All possible errors will be returned as [`Error::InvalidParams`]
    /// during query execution (`execute()`, `fetch()`, etc.).
    ///
    /// WARNING: This means that the query must not have any extra `?`, even if
    /// they are in a string literal! Use `??` to have plain `?` in query.
    ///
    /// [`Serialize`]: serde::Serialize
    /// [`Identifier`]: crate::sql::Identifier
    #[track_caller]
    pub fn bind(mut self, value: impl Bind) -> Self {
        self.sql.bind_arg(value);
        self
    }

    /// Attaches an [external table] to be processed as a temporary table for
    /// this query only.
    ///
    /// The table is materialized once per request, so referencing it several
    /// times in the query is cheaper than a `WITH` subquery, which the server
    /// re-scans per reference. Call this repeatedly to attach several tables.
    ///
    /// [external table]: crate::external_data::ExternalTable
    pub fn with_external_table(mut self, table: ExternalTable) -> Self {
        self.external_tables.push(table);
        self
    }

    /// Executes the query.
    pub async fn execute(self) -> Result<()> {
        // Enter the span for the `self.do_execute()` call
        let span = self.make_span(None);

        async {
            let mut response = self
                .do_execute(None)
                .inspect_err(|e| e.record_in_current_span("error executing query"))?;

            response
                .finish()
                .await
                .inspect_err(|e| e.record_in_current_span("response error"))
        }
        .instrument(span)
        .await
    }

    /// Executes the query, returning a [`RowCursor`] to obtain results.
    ///
    /// # Example
    ///
    /// ```
    /// # async fn example() -> clickhouse::error::Result<()> {
    /// #[derive(clickhouse::Row, serde::Deserialize)]
    /// struct MyRow<'a> {
    ///     no: u32,
    ///     name: &'a str,
    /// }
    ///
    /// let mut cursor = clickhouse::Client::default()
    ///     .query("SELECT ?fields FROM some WHERE no BETWEEN 0 AND 1")
    ///     .fetch::<MyRow<'_>>()?;
    ///
    /// while let Some(MyRow { name, no }) = cursor.next().await? {
    ///     println!("{name}: {no}");
    /// }
    /// # Ok(()) }
    /// ```
    pub fn fetch<T: Row>(mut self) -> Result<RowCursor<T>> {
        let validation = self.client.get_validation();
        let format = if validation {
            formats::ROW_BINARY_WITH_NAMES_AND_TYPES
        } else {
            formats::ROW_BINARY
        };

        let span = self.make_span(Some(format)).entered();

        self.sql.bind_fields::<T>();

        let response = self
            .do_execute(Some(format))
            .inspect_err(|e| e.record_in_current_span("error executing fetch"))?;

        Ok(RowCursor::new(response, validation, span.exit()))
    }

    /// Executes the query and returns just a single row.
    ///
    /// Note that `T` must be owned.
    pub async fn fetch_one<T>(self) -> Result<T>
    where
        T: RowOwned + RowRead,
    {
        match self.fetch::<T>()?.next().await {
            Ok(Some(row)) => Ok(row),
            Ok(None) => Err(Error::RowNotFound),
            Err(err) => Err(err),
        }
    }

    /// Executes the query and returns at most one row.
    ///
    /// Note that `T` must be owned.
    pub async fn fetch_optional<T>(self) -> Result<Option<T>>
    where
        T: RowOwned + RowRead,
    {
        self.fetch::<T>()?.next().await
    }

    /// Executes the query and returns all the generated results,
    /// collected into a Vec.
    ///
    /// Note that `T` must be owned.
    pub async fn fetch_all<T>(self) -> Result<Vec<T>>
    where
        T: RowOwned + RowRead,
    {
        let mut result = Vec::new();
        let mut cursor = self.fetch::<T>()?;

        while let Some(row) = cursor.next().await? {
            result.push(row);
        }

        Ok(result)
    }

    /// Executes the query, returning a [`BytesCursor`] to obtain results as raw
    /// bytes containing data in the [provided format].
    ///
    /// [provided format]: https://clickhouse.com/docs/en/interfaces/formats
    pub fn fetch_bytes(self, format: impl AsRef<str>) -> Result<BytesCursor> {
        let format = format.as_ref();

        let span = self.make_span(Some(format)).entered();

        let response = self.do_execute(Some(format))?;
        Ok(BytesCursor::new(response, span.exit()))
    }

    pub(crate) fn make_span(&self, response_format: Option<&str>) -> tracing::Span {
        // https://opentelemetry.io/docs/specs/semconv/db/sql/
        // TODO: write our own Semantic Conventions for ClickHouse
        tracing::info_span!(
            "clickhouse.query",
            // OTel conventional fields
            // Note that `Empty` or `Option::None` fields are not reported,
            // so we can avoid adding noise to logs when the `opentelemetry` feature is disabled.
            otel.status_code = tracing::field::Empty,
            otel.kind = cfg!(feature = "opentelemetry").then_some("client"),
            error.type = tracing::field::Empty,
            db.system.name = cfg!(feature = "opentelemetry").then_some("clickhouse"),
            // Only log full query text at TRACE level
            // Important that this is taken before client-side parameters are populated
            // FIXME: we can't use `enabled!` due to https://github.com/tokio-rs/tracing/issues/2448
            // but we don't want to log the full query at all verbosity levels.
            // db.query.text = tracing::enabled!(tracing::Level::TRACE).then(|| self.sql.to_string()),
            // TODO: generate summary
            db.query.summary = tracing::field::Empty,
            db.response.status_code = tracing::field::Empty,
            db.response.returned_rows = tracing::field::Empty,
            // ClickHouse-specific extension fields
            clickhouse.request.session_id = self.client.get_setting(settings::SESSION_ID),
            clickhouse.request.query_id = self.client.get_setting(settings::QUERY_ID),
            clickhouse.response.received_bytes = tracing::field::Empty,
            clickhouse.response.decoded_bytes = tracing::field::Empty,
            clickhouse.response.format = response_format,
        )
    }

    pub(crate) fn do_execute(self, default_format: Option<&str>) -> Result<Response> {
        let query = self.sql.finish()?;

        let mut url =
            Url::parse(&self.client.url).map_err(|err| Error::InvalidParams(Box::new(err)))?;
        let mut pairs = url.query_pairs_mut();
        pairs.clear();

        if let Some(format) = default_format {
            pairs.append_pair(settings::DEFAULT_FORMAT, format);
        }

        if let Some(database) = &self.client.database {
            pairs.append_pair(settings::DATABASE, database);
        }

        if self.client.compression.is_enabled() {
            #[cfg(feature = "zstd")]
            if matches!(self.client.compression, crate::Compression::Zstd(_)) {
                pairs.append_pair(settings::ENABLE_HTTP_COMPRESSION, "1");
            } else {
                pairs.append_pair(settings::COMPRESS, "1");
            }

            #[cfg(not(feature = "zstd"))]
            pairs.append_pair(settings::COMPRESS, "1");
        }

        for (name, value) in &self.client.settings {
            pairs.append_pair(name, value);
        }

        pairs.extend_pairs(self.client.roles.iter().map(|role| (settings::ROLE, role)));

        // External tables carry their structure and format as query params.
        // Their data travels in the multipart body built below.
        for table in &self.external_tables {
            pairs.append_pair(&format!("{}_structure", table.name()), table.structure());
            pairs.append_pair(&format!("{}_format", table.name()), table.format_name());
        }

        drop(pairs);

        let mut builder = Request::builder().method(Method::POST).uri(url.as_str());
        builder = with_request_headers(builder, &self.client.headers, &self.client.products_info);
        builder = with_authentication(builder, &self.client.authentication);

        #[cfg(feature = "zstd")]
        if matches!(self.client.compression, crate::Compression::Zstd(_)) {
            builder = builder.header("Accept-Encoding", "zstd");
        }

        // Without external data the query is the whole body. With it, the query
        // becomes a `query` form field alongside one file part per table.
        let body = if self.external_tables.is_empty() {
            builder = builder.header(CONTENT_LENGTH, query.len().to_string());
            RequestBody::full(query)
        } else {
            let multipart = external_data::build_multipart(&query, &self.external_tables);
            builder = builder
                .header(CONTENT_LENGTH, multipart.content_length().to_string())
                .header(
                    CONTENT_TYPE,
                    format!("multipart/form-data; boundary={}", multipart.boundary),
                );
            RequestBody::multi(multipart.frames)
        };

        let request = builder.body(body).map_err(|err| {
            let err = Error::InvalidParams(Box::new(err));
            err.record_in_current_span("invalid params in query");
            err
        })?;

        let future = self.client.http.request(request);
        Ok(Response::new(future, self.client.compression))
    }

    /// Configure the [roles] to use when executing this query.
    ///
    /// Overrides any roles previously set by this method, [`Query::with_setting`],
    /// [`Client::with_roles`] or [`Client::with_setting`].
    ///
    /// An empty iterator may be passed to clear the set roles.
    ///
    /// [roles]: https://clickhouse.com/docs/operations/access-rights#role-management
    pub fn with_roles(self, roles: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            client: self.client.with_roles(roles),
            ..self
        }
    }

    /// Clear any explicit [roles] previously set on this `Query` or inherited from [`Client`].
    ///
    /// Overrides any roles previously set by [`Query::with_roles`], [`Query::with_setting`],
    /// [`Client::with_roles`] or [`Client::with_setting`].
    ///
    /// [roles]: https://clickhouse.com/docs/operations/access-rights#role-management
    pub fn with_default_roles(self) -> Self {
        Self {
            client: self.client.with_default_roles(),
            ..self
        }
    }

    /// Similar to [`Client::with_option`], but for this particular query only.
    #[deprecated(since = "0.14.3", note = "please use `with_setting` instead")]
    pub fn with_option(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.client.set_setting(name, value);
        self
    }

    /// Similar to [`Client::with_setting`], but for this particular query only.
    pub fn with_setting(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.client.set_setting(name, value);
        self
    }

    // Used in `clickhouse-ext-arrow` to track Arrow adoption.
    /// Similar to [`Client::with_product_info()`], but for this query only.
    pub fn with_product_info(
        self,
        product_name: impl Into<String>,
        product_version: impl Into<String>,
    ) -> Self {
        Self {
            client: self.client.with_product_info(product_name, product_version),
            ..self
        }
    }

    /// Specify server side parameter for query.
    ///
    /// In queries, you can reference params as {name: type} e.g. {val: Int32}.
    pub fn param(mut self, name: &str, value: impl Serialize) -> Self {
        let mut param = String::from("");
        if let Err(err) = ser::write_param(&mut param, &value) {
            self.sql = SqlBuilder::Failed(format!("invalid param: {err}"));
            self
        } else {
            self.with_setting(format!("param_{name}"), param)
        }
    }
}
