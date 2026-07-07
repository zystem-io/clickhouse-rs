use bytes::Bytes;
use futures_channel::mpsc;
use futures_util::{SinkExt, Stream};
use hyper::body::{Body, Frame, SizeHint};
use std::ops::ControlFlow;
use std::{
    error::Error as StdError,
    mem,
    pin::Pin,
    task::{Context, Poll},
};
// === RequestBody ===

pub struct RequestBody(Inner);

enum Inner {
    Full(Bytes),
    // Pre-assembled frames yielded in order. Lets a multipart body chain its
    // framing with each payload's original `Bytes`. Draining the iterator drops
    // each frame as it is sent. The size stays exact as the sum of frame lengths.
    Multi(std::vec::IntoIter<Bytes>),
    Chunked(mpsc::Receiver<Message>),
}

enum Message {
    Chunk(Bytes),
    Abort,
}

impl RequestBody {
    pub(crate) fn full(content: String) -> Self {
        Self(Inner::Full(Bytes::from(content)))
    }

    pub(crate) fn multi(frames: impl IntoIterator<Item = Bytes>) -> Self {
        Self(Inner::Multi(
            frames.into_iter().collect::<Vec<_>>().into_iter(),
        ))
    }

    pub(crate) fn chunked() -> (ChunkSender, Self) {
        let (tx, rx) = mpsc::channel(0); // each sender gets a guaranteed slot
        let sender = ChunkSender(tx);
        (sender, Self(Inner::Chunked(rx)))
    }
}

impl Body for RequestBody {
    type Data = Bytes;
    type Error = Box<dyn StdError + Send + Sync>;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        match &mut self.get_mut().0 {
            Inner::Full(bytes) if bytes.is_empty() => Poll::Ready(None),
            Inner::Full(bytes) => Poll::Ready(Some(Ok(Frame::data(mem::take(bytes))))),
            Inner::Multi(frames) => match frames.next() {
                Some(bytes) => Poll::Ready(Some(Ok(Frame::data(bytes)))),
                None => Poll::Ready(None),
            },
            Inner::Chunked(rx) => match Pin::new(rx).poll_next(cx) {
                Poll::Ready(Some(Message::Chunk(bytes))) => {
                    Poll::Ready(Some(Ok(Frame::data(bytes))))
                }
                Poll::Ready(Some(Message::Abort)) => Poll::Ready(Some(Err("aborted".into()))),
                Poll::Ready(None) => Poll::Ready(None),
                Poll::Pending => Poll::Pending,
            },
        }
    }

    fn is_end_stream(&self) -> bool {
        match &self.0 {
            Inner::Full(bytes) => bytes.is_empty(),
            Inner::Multi(frames) => frames.as_slice().is_empty(),
            Inner::Chunked(_) => false, // default `Body::is_end_stream()`
        }
    }

    fn size_hint(&self) -> SizeHint {
        match &self.0 {
            Inner::Full(bytes) => SizeHint::with_exact(bytes.len() as u64),
            Inner::Multi(frames) => {
                SizeHint::with_exact(frames.as_slice().iter().map(Bytes::len).sum::<usize>() as u64)
            }
            Inner::Chunked(_) => SizeHint::default(), // default `Body::size_hint()`
        }
    }
}

// === ChunkSender ===

pub(crate) struct ChunkSender(mpsc::Sender<Message>);

impl ChunkSender {
    #[allow(dead_code)] // YAGNI?
    pub(crate) async fn send(&mut self, chunk: Bytes) -> bool {
        self.0.send(Message::Chunk(chunk)).await.is_ok()
    }

    #[inline(always)]
    pub(crate) fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<bool> {
        self.0.poll_ready(cx).map(|res| res.is_ok())
    }

    #[inline(always)]
    pub(crate) fn try_send(
        &mut self,
        chunk: Bytes,
    ) -> ControlFlow<Result<(), &'static str>, Bytes> {
        self.0.try_send(Message::Chunk(chunk)).map_or_else(
            |e| {
                if e.is_full() {
                    let Message::Chunk(bytes) = e.into_inner() else {
                        unreachable!()
                    };

                    ControlFlow::Continue(bytes)
                } else {
                    ControlFlow::Break(Err("channel closed"))
                }
            },
            |()| ControlFlow::Break(Ok(())),
        )
    }

    pub(crate) fn abort(&self) {
        // `clone()` allows to send even if the channel is full.
        let _ = self.0.clone().try_send(Message::Abort);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::task::Waker;

    fn drain(mut body: RequestBody) -> Vec<Bytes> {
        let mut cx = Context::from_waker(Waker::noop());
        let mut out = vec![];
        loop {
            match Pin::new(&mut body).poll_frame(&mut cx) {
                Poll::Ready(Some(Ok(frame))) => {
                    out.push(frame.into_data().expect("multi yields only data frames"))
                }
                Poll::Ready(None) => break,
                Poll::Ready(Some(Err(_))) => panic!("multi must not yield an error"),
                Poll::Pending => panic!("multi must never be pending"),
            }
        }
        out
    }

    #[test]
    fn multi_yields_frames_in_order() {
        let frames = [
            Bytes::from_static(b"a"),
            Bytes::from_static(b"bb"),
            Bytes::from_static(b"ccc"),
        ];
        let got = drain(RequestBody::multi(frames.clone()));
        assert_eq!(
            got, frames,
            "frames must come out in the order they were queued"
        );
    }

    #[test]
    fn multi_size_hint_and_end_stream() {
        let frames = [Bytes::from_static(b"a"), Bytes::from_static(b"bb")];
        let body = RequestBody::multi(frames);

        assert_eq!(
            body.size_hint().exact(),
            Some(3),
            "size hint must sum the frame lengths"
        );
        assert!(
            !body.is_end_stream(),
            "must not report end while frames are queued"
        );

        assert!(drain(body).len() == 2, "both frames must be yielded");
        assert!(
            RequestBody::multi([]).is_end_stream(),
            "an empty multi body reports end of stream"
        );
    }
}
