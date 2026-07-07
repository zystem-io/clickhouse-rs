use serde::Deserialize;

use clickhouse::Row;
use clickhouse::external_data::ExternalTable;

use crate::get_client;

#[derive(Debug, Row, Deserialize)]
struct User {
    id: u64,
    name: String,
}

#[tokio::test]
async fn fetches_from_external_table() {
    let client = get_client();

    let rows = client
        .query("SELECT ?fields FROM users ORDER BY id")
        .with_external_table(
            ExternalTable::new("users", "1\tAlice\n2\tBob\n", "id UInt64, name String").unwrap(),
        )
        .fetch_all::<User>()
        .await
        .unwrap();

    let got: Vec<_> = rows.iter().map(|r| (r.id, r.name.as_str())).collect();
    assert_eq!(got, [(1, "Alice"), (2, "Bob")], "unexpected rows: {rows:?}");
}

/// A temporary table can be referenced several times in one query, unlike a
/// data literal. This is the motivating use case over a `WITH` subquery.
#[tokio::test]
async fn referenced_multiple_times() {
    let client = get_client();

    let count = client
        .query("SELECT count() FROM users AS a JOIN users AS b ON a.id = b.id")
        .with_external_table(
            ExternalTable::new("users", "1\tA\n2\tB\n3\tC\n", "id UInt64, name String").unwrap(),
        )
        .fetch_one::<u64>()
        .await
        .unwrap();

    assert_eq!(
        count, 3,
        "self-join over the temp table should match 3 rows"
    );
}

#[tokio::test]
async fn multiple_tables_joined() {
    let client = get_client();

    let name = client
        .query(
            "SELECT u.name FROM users AS u \
             JOIN roles AS r ON u.id = r.user_id WHERE r.role = 'admin'",
        )
        .with_external_table(
            ExternalTable::new("users", "1\tAlice\n2\tBob\n", "id UInt64, name String")
                .unwrap()
                .with_format("TSV"),
        )
        .with_external_table(
            ExternalTable::new("roles", "2,admin\n1,guest\n", "user_id UInt64, role String")
                .unwrap()
                .with_format("CSV"),
        )
        .fetch_one::<String>()
        .await
        .unwrap();

    assert_eq!(name, "Bob", "admin role belongs to Bob");
}

#[tokio::test]
async fn empty_external_table() {
    let client = get_client();

    let rows = client
        .query("SELECT ?fields FROM users ORDER BY id")
        .with_external_table(ExternalTable::new("users", "", "id UInt64, name String").unwrap())
        .fetch_all::<User>()
        .await
        .unwrap();

    assert!(rows.is_empty(), "empty table must yield no rows: {rows:?}");
}

#[tokio::test]
async fn json_each_row_format() {
    let client = get_client();

    let rows = client
        .query("SELECT ?fields FROM users ORDER BY id")
        .with_external_table(
            ExternalTable::new(
                "users",
                "{\"id\":1,\"name\":\"Alice\"}\n{\"id\":2,\"name\":\"Bob\"}\n",
                "id UInt64, name String",
            )
            .unwrap()
            .with_format("JSONEachRow"),
        )
        .fetch_all::<User>()
        .await
        .unwrap();

    let got: Vec<_> = rows.iter().map(|r| (r.id, r.name.as_str())).collect();
    assert_eq!(got, [(1, "Alice"), (2, "Bob")], "unexpected rows: {rows:?}");
}
