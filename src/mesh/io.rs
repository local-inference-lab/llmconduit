#![allow(dead_code)]

use crate::error::{AppError, AppResult};
pub use crate::mesh::protocol::{Admission, RequestOpen, StreamOpen, SwitchModelResponse};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio_util::bytes::{Buf, BytesMut};

pub async fn write_request_open<W>(writer: &mut W, open: &RequestOpen) -> AppResult<()>
where
    W: AsyncWrite + Unpin,
{
    write_stream_open(writer, &StreamOpen::Inference(open.clone())).await
}

pub async fn write_stream_open<W>(writer: &mut W, open: &StreamOpen) -> AppResult<()>
where
    W: AsyncWrite + Unpin,
{
    let bytes = serde_json::to_vec(open).map_err(|err| {
        AppError::internal(format!("failed to encode mesh request preface: {err}"))
    })?;
    write_len_prefixed(
        writer,
        &bytes,
        crate::mesh::protocol::MAX_REQUEST_PREFACE_BYTES,
    )
    .await
}

pub async fn read_request_open<R>(reader: &mut R) -> AppResult<RequestOpen>
where
    R: AsyncRead + Unpin,
{
    match read_stream_open(reader).await? {
        StreamOpen::Inference(open) => Ok(open),
        StreamOpen::SwitchModel(_) | StreamOpen::UnloadModel(_) => {
            Err(AppError::bad_request("expected mesh inference stream"))
        }
    }
}

pub async fn read_stream_open<R>(reader: &mut R) -> AppResult<StreamOpen>
where
    R: AsyncRead + Unpin,
{
    let bytes = read_len_prefixed(reader, crate::mesh::protocol::MAX_REQUEST_PREFACE_BYTES).await?;
    serde_json::from_slice(&bytes)
        .map_err(|err| AppError::bad_request(format!("invalid mesh stream preface: {err}")))
}

pub async fn write_switch_response<W>(
    writer: &mut W,
    response: &SwitchModelResponse,
) -> AppResult<()>
where
    W: AsyncWrite + Unpin,
{
    let bytes = serde_json::to_vec(response).map_err(|err| {
        AppError::internal(format!("failed to encode mesh switch response: {err}"))
    })?;
    write_len_prefixed(
        writer,
        &bytes,
        crate::mesh::protocol::MAX_CONTROL_FRAME_BYTES,
    )
    .await
}

pub async fn read_switch_response<R>(reader: &mut R) -> AppResult<SwitchModelResponse>
where
    R: AsyncRead + Unpin,
{
    let bytes = read_len_prefixed(reader, crate::mesh::protocol::MAX_CONTROL_FRAME_BYTES).await?;
    serde_json::from_slice(&bytes)
        .map_err(|err| AppError::bad_request(format!("invalid mesh switch response: {err}")))
}

pub async fn write_admission<W>(writer: &mut W, admission: &Admission) -> AppResult<()>
where
    W: AsyncWrite + Unpin,
{
    let bytes = serde_json::to_vec(admission)
        .map_err(|err| AppError::internal(format!("failed to encode mesh admission: {err}")))?;
    write_len_prefixed(
        writer,
        &bytes,
        crate::mesh::protocol::MAX_REQUEST_PREFACE_BYTES,
    )
    .await
}

pub async fn read_admission<R>(reader: &mut R) -> AppResult<Admission>
where
    R: AsyncRead + Unpin,
{
    let bytes = read_len_prefixed(reader, crate::mesh::protocol::MAX_REQUEST_PREFACE_BYTES).await?;
    serde_json::from_slice(&bytes)
        .map_err(|err| AppError::bad_request(format!("invalid mesh admission frame: {err}")))
}

async fn write_len_prefixed<W>(writer: &mut W, bytes: &[u8], max_len: usize) -> AppResult<()>
where
    W: AsyncWrite + Unpin,
{
    if bytes.len() > max_len {
        return Err(AppError::bad_request(format!(
            "mesh frame exceeds {max_len} byte limit"
        )));
    }
    writer
        .write_all(&(bytes.len() as u32).to_be_bytes())
        .await
        .map_err(|err| AppError::upstream(format!("failed to write mesh frame length: {err}")))?;
    writer
        .write_all(bytes)
        .await
        .map_err(|err| AppError::upstream(format!("failed to write mesh frame body: {err}")))?;
    writer
        .flush()
        .await
        .map_err(|err| AppError::upstream(format!("failed to flush mesh frame: {err}")))
}

async fn read_len_prefixed<R>(reader: &mut R, max_len: usize) -> AppResult<Vec<u8>>
where
    R: AsyncRead + Unpin,
{
    let mut len = [0_u8; 4];
    reader
        .read_exact(&mut len)
        .await
        .map_err(|err| AppError::upstream(format!("failed to read mesh frame length: {err}")))?;
    let len = u32::from_be_bytes(len) as usize;
    if len > max_len {
        return Err(AppError::bad_request(format!(
            "mesh frame length {len} exceeds {max_len} byte limit"
        )));
    }
    let mut bytes = vec![0_u8; len];
    reader
        .read_exact(&mut bytes)
        .await
        .map_err(|err| AppError::upstream(format!("failed to read mesh frame body: {err}")))?;
    Ok(bytes)
}

pub async fn read_http_response_head<R>(reader: &mut R, max_head_bytes: usize) -> AppResult<Vec<u8>>
where
    R: AsyncRead + Unpin,
{
    let mut head = BytesMut::with_capacity(1024);
    let mut one = [0_u8; 1];
    while head.len() < max_head_bytes {
        let read = reader.read(&mut one).await.map_err(|err| {
            AppError::upstream(format!("failed to read mesh HTTP response head: {err}"))
        })?;
        if read == 0 {
            return Err(AppError::upstream(
                "mesh HTTP response ended before headers completed",
            ));
        }
        head.extend_from_slice(&one);
        if head.len() >= 4 && &head[head.len() - 4..] == b"\r\n\r\n" {
            return Ok(head.to_vec());
        }
    }
    Err(AppError::upstream(format!(
        "mesh HTTP response head exceeds {max_head_bytes} byte limit"
    )))
}

pub fn split_http_response_head(head: &[u8]) -> AppResult<(http::StatusCode, http::HeaderMap)> {
    let text = std::str::from_utf8(head)
        .map_err(|err| AppError::upstream(format!("invalid mesh HTTP response head: {err}")))?;
    let mut lines = text.split("\r\n");
    let status_line = lines
        .next()
        .ok_or_else(|| AppError::upstream("missing mesh HTTP status line"))?;
    let mut parts = status_line.splitn(3, ' ');
    let _version = parts.next();
    let status = parts
        .next()
        .ok_or_else(|| AppError::upstream("missing mesh HTTP status code"))?
        .parse::<u16>()
        .map_err(|err| AppError::upstream(format!("invalid mesh HTTP status code: {err}")))?;
    let status = http::StatusCode::from_u16(status)
        .map_err(|err| AppError::upstream(format!("invalid mesh HTTP status code: {err}")))?;
    let mut headers = http::HeaderMap::new();
    for line in lines {
        if line.is_empty() {
            break;
        }
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        if let (Ok(name), Ok(value)) = (
            http::HeaderName::from_bytes(name.as_bytes()),
            http::HeaderValue::from_str(value.trim()),
        ) {
            headers.append(name, value);
        }
    }
    Ok((status, headers))
}

pub fn content_length(headers: &http::HeaderMap) -> Option<usize> {
    headers
        .get(http::header::CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse().ok())
}

pub fn is_chunked(headers: &http::HeaderMap) -> bool {
    headers
        .get(http::header::TRANSFER_ENCODING)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.to_ascii_lowercase().contains("chunked"))
}

pub const MAX_HTTP_CHUNK_LINE_BYTES: usize = 1024;
pub const MAX_HTTP_CHUNK_BYTES: usize = 16 * 1024 * 1024;

pub fn drain_http_chunk(
    buf: &mut BytesMut,
    max_chunk_bytes: usize,
) -> AppResult<Option<tokio_util::bytes::Bytes>> {
    let Some(line_end) = buf.windows(2).position(|window| window == b"\r\n") else {
        if buf.len() > MAX_HTTP_CHUNK_LINE_BYTES {
            return Err(AppError::upstream(format!(
                "mesh HTTP chunk size line exceeds {MAX_HTTP_CHUNK_LINE_BYTES} byte limit"
            )));
        }
        return Ok(None);
    };
    if line_end > MAX_HTTP_CHUNK_LINE_BYTES {
        return Err(AppError::upstream(format!(
            "mesh HTTP chunk size line exceeds {MAX_HTTP_CHUNK_LINE_BYTES} byte limit"
        )));
    }
    let line = std::str::from_utf8(&buf[..line_end])
        .map_err(|err| AppError::upstream(format!("invalid mesh HTTP chunk size: {err}")))?;
    let size = usize::from_str_radix(
        line.split(';')
            .next()
            .ok_or_else(|| AppError::upstream("missing mesh HTTP chunk size"))?
            .trim(),
        16,
    )
    .map_err(|err| AppError::upstream(format!("invalid mesh HTTP chunk size: {err}")))?;
    let max_chunk_bytes = max_chunk_bytes.min(MAX_HTTP_CHUNK_BYTES);
    if size > max_chunk_bytes {
        return Err(AppError::upstream(format!(
            "mesh HTTP chunk declares {size} bytes, maximum is {max_chunk_bytes}"
        )));
    }
    let frame_len = line_end
        .checked_add(2)
        .and_then(|value| value.checked_add(size))
        .and_then(|value| value.checked_add(2))
        .ok_or_else(|| AppError::upstream("mesh HTTP chunk frame length overflow"))?;
    if buf.len() < frame_len {
        return Ok(None);
    }
    if &buf[frame_len - 2..frame_len] != b"\r\n" {
        return Err(AppError::upstream("mesh HTTP chunk missing trailing CRLF"));
    }
    buf.advance(line_end + 2);
    // Hand out the chunk without copying; the buffer keeps only what follows.
    let chunk = buf.split_to(size).freeze();
    buf.advance(2);
    Ok(Some(chunk))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::duplex;
    use uuid::Uuid;

    #[tokio::test]
    async fn request_preface_round_trips_with_bound() {
        let (mut a, mut b) = duplex(256);
        let open = RequestOpen {
            protocol_version: crate::mesh::protocol::REQUEST_PROTOCOL_VERSION,
            request_id: Uuid::nil(),
            resource_id: "primary".to_string(),
        };
        let write = tokio::spawn(async move { write_request_open(&mut a, &open).await });
        let got = read_request_open(&mut b).await.expect("read");
        write.await.expect("join").expect("write");
        assert_eq!(got.resource_id, "primary");
    }

    #[tokio::test]
    async fn switch_response_round_trips_inventory_larger_than_request_preface_limit() {
        let (mut a, mut b) = duplex(crate::mesh::protocol::MAX_CONTROL_FRAME_BYTES * 2);
        let response = SwitchModelResponse {
            request_id: Uuid::nil(),
            model_id: "qwen3-flash".to_string(),
            accepted: true,
            changed: true,
            error: None,
            model_switching: Some(crate::mesh::protocol::ModelSwitchingAdvertisement {
                provider: "lil-fleet".to_string(),
                models: (0..32)
                    .map(|index| {
                        crate::mesh::protocol::SwitchableModelAdvertisement::legacy(
                            format!("model-{index}"),
                            Some("x".repeat(256)),
                            "unloaded",
                            "unloaded",
                            1,
                            Vec::new(),
                        )
                    })
                    .collect(),
                revision: 2,
            }),
            error_status: None,
        };
        let write = tokio::spawn(async move { write_switch_response(&mut a, &response).await });
        let got = read_switch_response(&mut b).await.expect("read response");
        write.await.expect("join").expect("write response");
        assert_eq!(got.model_id, "qwen3-flash");
        assert_eq!(got.model_switching.expect("inventory").models.len(), 32);
    }

    #[test]
    fn parses_http_response_head() {
        let head = b"HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\n\r\n";
        let (status, headers) = split_http_response_head(head).expect("head");
        assert_eq!(status, http::StatusCode::OK);
        assert_eq!(headers[http::header::CONTENT_TYPE], "text/event-stream");
    }

    #[test]
    fn chunk_parser_rejects_oversized_size_lines() {
        let mut buf = BytesMut::from(&b"1"[..]);
        buf.resize(MAX_HTTP_CHUNK_LINE_BYTES + 1, b'1');
        assert!(drain_http_chunk(&mut buf, MAX_HTTP_CHUNK_BYTES).is_err());
    }

    #[test]
    fn chunk_parser_rejects_oversized_declared_chunks() {
        let mut buf = BytesMut::from(format!("{:x}\r\n", MAX_HTTP_CHUNK_BYTES + 1).as_bytes());
        assert!(drain_http_chunk(&mut buf, MAX_HTTP_CHUNK_BYTES).is_err());
    }

    #[test]
    fn chunk_parser_honors_configured_chunk_limit() {
        let mut buf = BytesMut::from(b"6\r\nhello!\r\n".as_slice());
        assert!(drain_http_chunk(&mut buf, 5).is_err());
    }

    #[test]
    fn chunk_parser_waits_for_bounded_incomplete_chunk() {
        let mut buf = BytesMut::from(b"5\r\nhe".as_slice());
        assert!(
            drain_http_chunk(&mut buf, MAX_HTTP_CHUNK_BYTES)
                .expect("bounded")
                .is_none()
        );
    }
}
