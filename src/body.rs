//! Request bodies as [`ByteSource`]s: plain, and aws-chunked (signed or unsigned,
//! with optional trailing checksums).

use async_trait::async_trait;
use bytes::{Buf, Bytes, BytesMut};
use http_body::Body;
use http_body_util::BodyExt;
use sha2::{Digest, Sha256};

use crate::auth::ChunkSigner;
use crate::checksum::{Checksum, ChecksumAlgo};
use crate::error::{ErrorCode, S3Error, S3Result};
use crate::storage::ByteSource;

const MAX_LINE: usize = 4096;
/// A request body that sends nothing for this long is abandoned.
const BODY_IDLE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);

/// Read the next data frame of a body, skipping HTTP trailer frames.
async fn next_data<B>(body: &mut B) -> S3Result<Option<Bytes>>
where
    B: Body<Data = Bytes> + Unpin + Send,
    B::Error: std::fmt::Display,
{
    loop {
        let frame = tokio::time::timeout(BODY_IDLE_TIMEOUT, body.frame())
            .await
            .map_err(|_| S3Error::msg(ErrorCode::IncompleteBody, "Timed out waiting for the request body"))?;
        match frame {
            None => return Ok(None),
            Some(Err(e)) => return Err(S3Error::msg(ErrorCode::IncompleteBody, format!("reading request body: {e}"))),
            Some(Ok(f)) => {
                if let Ok(d) = f.into_data()
                    && !d.is_empty()
                {
                    return Ok(Some(d));
                }
            }
        }
    }
}

/// A plain body.
pub struct PlainSource<B> {
    body: B,
}

impl<B> PlainSource<B> {
    pub fn new(body: B) -> Self {
        PlainSource { body }
    }
}

#[async_trait]
impl<B> ByteSource for PlainSource<B>
where
    B: Body<Data = Bytes> + Unpin + Send,
    B::Error: std::fmt::Display,
{
    async fn next_chunk(&mut self) -> S3Result<Option<Bytes>> {
        next_data(&mut self.body).await
    }
}

/// Read a whole (small) body, enforcing a size limit.
pub async fn read_limited<B>(body: &mut B, limit: usize) -> S3Result<Bytes>
where
    B: Body<Data = Bytes> + Unpin + Send,
    B::Error: std::fmt::Display,
{
    let mut out = BytesMut::new();
    while let Some(d) = next_data(body).await? {
        if out.len() + d.len() > limit {
            return Err(S3Error::msg(ErrorCode::InvalidRequest, "Request body is too large"));
        }
        out.extend_from_slice(&d);
    }
    Ok(out.freeze())
}

#[derive(Debug, PartialEq)]
enum State {
    /// Expecting "<hex size>[;chunk-signature=<sig>]\r\n"
    Header,
    /// Inside chunk data, with this many bytes left.
    Data(usize),
    /// Expecting the "\r\n" that ends a chunk's data.
    DataEnd,
    /// After the final chunk, reading trailer lines until an empty one.
    Trailers,
    Done,
}

/// Decoder for `Content-Encoding: aws-chunked` bodies.
pub struct ChunkedSource<B> {
    body: B,
    buf: BytesMut,
    eof: bool,
    state: State,
    signer: Option<ChunkSigner>,
    trailer: bool,
    /// Signature of the chunk being read, and the running hash of its data.
    chunk_sig: Option<String>,
    chunk_hash: Sha256,
    trailers: Vec<(String, String)>,
    trailing: Option<Checksum>,
}

impl<B> ChunkedSource<B>
where
    B: Body<Data = Bytes> + Unpin + Send,
    B::Error: std::fmt::Display,
{
    pub fn new(body: B, signer: Option<ChunkSigner>, trailer: bool) -> Self {
        ChunkedSource {
            body,
            buf: BytesMut::new(),
            eof: false,
            state: State::Header,
            signer,
            trailer,
            chunk_sig: None,
            chunk_hash: Sha256::new(),
            trailers: Vec::new(),
            trailing: None,
        }
    }

    /// Read more input into the buffer. Returns false at end of input.
    async fn fill(&mut self) -> S3Result<bool> {
        if self.eof {
            return Ok(false);
        }
        match next_data(&mut self.body).await? {
            Some(d) => {
                self.buf.extend_from_slice(&d);
                Ok(true)
            }
            None => {
                self.eof = true;
                Ok(false)
            }
        }
    }

    /// Take one "\r\n"-terminated line (without the terminator). None at clean EOF.
    async fn line(&mut self) -> S3Result<Option<String>> {
        loop {
            if let Some(i) = self.buf.windows(2).position(|w| w == b"\r\n") {
                let line = self.buf.split_to(i + 2);
                let s = std::str::from_utf8(&line[..i]).map_err(|_| malformed("invalid chunk header"))?;
                return Ok(Some(s.to_string()));
            }
            if self.buf.len() > MAX_LINE {
                return Err(malformed("chunk header too long"));
            }
            if !self.fill().await? {
                if self.buf.is_empty() {
                    return Ok(None);
                }
                // Tolerate a final line without "\r\n".
                let rest = self.buf.split();
                return Ok(Some(String::from_utf8_lossy(&rest).into_owned()));
            }
        }
    }

    fn start_chunk(&mut self, header: &str) -> S3Result<usize> {
        let (size, ext) = header.split_once(';').unwrap_or((header, ""));
        let size = usize::from_str_radix(size.trim(), 16).map_err(|_| malformed("invalid chunk size"))?;
        self.chunk_sig = ext.trim().strip_prefix("chunk-signature=").map(|s| s.trim().to_string());
        if self.signer.is_some() && self.chunk_sig.is_none() {
            return Err(S3Error::msg(ErrorCode::SignatureDoesNotMatch, "Missing chunk signature"));
        }
        self.chunk_hash = Sha256::new();
        Ok(size)
    }

    fn finish_chunk(&mut self) -> S3Result<()> {
        if let Some(signer) = &mut self.signer {
            let hash = std::mem::take(&mut self.chunk_hash).finalize();
            signer.verify_chunk(&hash, self.chunk_sig.as_deref().unwrap_or(""))?;
        }
        Ok(())
    }

    async fn read_trailers(&mut self) -> S3Result<()> {
        let mut signature = None;
        while let Some(line) = self.line().await? {
            if line.is_empty() {
                break;
            }
            let (k, v) = line.split_once(':').ok_or_else(|| malformed("invalid trailer"))?;
            let (k, v) = (k.trim().to_ascii_lowercase(), v.trim().to_string());
            if k == "x-amz-trailer-signature" {
                signature = Some(v);
            } else {
                self.trailers.push((k, v));
            }
        }
        if let Some(signer) = &mut self.signer {
            let canonical: String = self.trailers.iter().map(|(k, v)| format!("{k}:{v}\n")).collect();
            signer.verify_trailer(&canonical, signature.as_deref().ok_or(ErrorCode::SignatureDoesNotMatch)?)?;
        }
        for (k, v) in &self.trailers {
            if let Some(algo) = ChecksumAlgo::from_header(k) {
                self.trailing = Some(Checksum { algo, value: v.clone() });
            }
        }
        Ok(())
    }
}

fn malformed(msg: &str) -> S3Error {
    S3Error::msg(ErrorCode::IncompleteBody, format!("Malformed aws-chunked body: {msg}"))
}

#[async_trait]
impl<B> ByteSource for ChunkedSource<B>
where
    B: Body<Data = Bytes> + Unpin + Send,
    B::Error: std::fmt::Display,
{
    async fn next_chunk(&mut self) -> S3Result<Option<Bytes>> {
        loop {
            match self.state {
                State::Done => return Ok(None),
                State::Header => {
                    let line = self.line().await?.ok_or_else(|| malformed("unexpected end of body"))?;
                    let size = self.start_chunk(&line)?;
                    if size == 0 {
                        self.finish_chunk()?;
                        if self.trailer {
                            self.state = State::Trailers;
                        } else {
                            // Optional final "\r\n".
                            let _ = self.line().await?;
                            self.state = State::Done;
                        }
                    } else {
                        self.state = State::Data(size);
                    }
                }
                State::Data(left) => {
                    if self.buf.is_empty() && !self.fill().await? {
                        return Err(malformed("unexpected end of chunk data"));
                    }
                    let n = left.min(self.buf.len());
                    let data = self.buf.split_to(n).freeze();
                    if self.signer.is_some() {
                        self.chunk_hash.update(&data);
                    }
                    self.state = if n == left { State::DataEnd } else { State::Data(left - n) };
                    return Ok(Some(data));
                }
                State::DataEnd => {
                    while self.buf.len() < 2 {
                        if !self.fill().await? {
                            return Err(malformed("unexpected end of chunk"));
                        }
                    }
                    if &self.buf[..2] != b"\r\n" {
                        return Err(malformed("chunk data longer than its declared size"));
                    }
                    self.buf.advance(2);
                    self.finish_chunk()?;
                    self.state = State::Header;
                }
                State::Trailers => {
                    self.read_trailers().await?;
                    self.state = State::Done;
                }
            }
        }
    }

    fn trailing_checksum(&self) -> Option<Checksum> {
        self.trailing.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http_body_util::Full;
    use std::collections::VecDeque;
    use std::pin::Pin;
    use std::task::{Context, Poll};

    /// A body that yields the given pieces as separate frames.
    struct Pieces(VecDeque<Bytes>);

    impl Body for Pieces {
        type Data = Bytes;
        type Error = std::io::Error;
        fn poll_frame(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Result<http_body::Frame<Bytes>, Self::Error>>> {
            Poll::Ready(self.0.pop_front().map(|b| Ok(http_body::Frame::data(b))))
        }
    }

    async fn decode(parts: &[&[u8]], trailer: bool) -> S3Result<(Vec<u8>, Option<Checksum>)> {
        let body = Pieces(parts.iter().map(|p| Bytes::copy_from_slice(p)).collect());
        let mut src = ChunkedSource::new(body, None, trailer);
        let mut out = Vec::new();
        while let Some(c) = src.next_chunk().await? {
            out.extend_from_slice(&c);
        }
        Ok((out, src.trailing_checksum()))
    }

    #[tokio::test]
    async fn unsigned_with_trailer() {
        let body = b"5\r\nhello\r\n6\r\n world\r\n0\r\nx-amz-checksum-crc32:DUoRhQ==\r\n\r\n";
        // split at awkward points
        let (a, rest) = body.split_at(3);
        let (b, c) = rest.split_at(9);
        let (data, sum) = decode(&[a, b, c], true).await.unwrap();
        assert_eq!(data, b"hello world");
        assert_eq!(sum.unwrap(), Checksum { algo: ChecksumAlgo::Crc32, value: "DUoRhQ==".into() });
    }

    #[tokio::test]
    async fn rejects_bad_framing() {
        assert!(decode(&[b"5\r\nhelloXX0\r\n\r\n"], false).await.is_err());
        assert!(decode(&[b"5\r\nhel"], false).await.is_err());
        assert!(decode(&[b"zz\r\n"], false).await.is_err());
    }

    #[tokio::test]
    async fn plain_limit() {
        let mut b = Full::new(Bytes::from_static(b"0123456789"));
        assert!(read_limited(&mut b, 5).await.is_err());
    }
}
