use crate::error::RZError;

pub const ROUTER_MAGIC: u8 = 0xFE;
pub const SHARD_MAGIC: u8 = 0xFF;

/// One decoded router frame.
pub struct RouterFrame<'a> {
    pub segment: &'a str,
    pub is_write: bool,
    /// Absolute offset of the 4-byte clrid inside the full buffer (shard header).
    pub clrid_offset: usize,
    pub original_clrid: u32,
    /// Total length of this router frame in `buf`.
    pub frame_len: usize,
}

/// Try to parse one complete router frame at the front of `buf`.
/// `None` = need more bytes.
pub fn try_decode_router(buf: &[u8]) -> Result<Option<RouterFrame<'_>>, RZError> {
    if buf.len() < 5 {
        return Ok(None);
    }
    if buf[0] != ROUTER_MAGIC {
        return Err(RZError::ParseError("bad router magic".into()));
    }

    let total_len = u32::from_le_bytes(buf[1..5].try_into().unwrap()) as usize;
    let frame_len = 5 + total_len;
    if buf.len() < frame_len {
        return Ok(None);
    }

    if total_len < 2 {
        return Err(RZError::ParseError("router total_len too small".into()));
    }

    let seg_len = buf[5] as usize;
    let seg_start = 6;
    let is_write_pos = seg_start + seg_len;
    if is_write_pos >= frame_len {
        return Err(RZError::ParseError("segment length out of range".into()));
    }

    let segment = std::str::from_utf8(&buf[seg_start..seg_start + seg_len])
        .map_err(|_| RZError::ParseError("segment not utf-8".into()))?;

    let is_write = buf[is_write_pos] == 0x01;
    let shard_start = is_write_pos + 1;

    if shard_start + 9 > frame_len || buf[shard_start] != SHARD_MAGIC {
        return Err(RZError::ParseError("missing shard magic".into()));
    }

    let original_clrid =
        u32::from_le_bytes(buf[shard_start + 1..shard_start + 5].try_into().unwrap());
    let clrid_offset = shard_start + 1;

    Ok(Some(RouterFrame {
        segment,
        is_write,
        clrid_offset,
        original_clrid,
        frame_len,
    }))
}

/// Build a constant error response frame.
/// Only replaces the CLRID with the provided one.
/// Error code is fixed: 1 = "unknown segment", 2 = "network error"
pub fn build_error_response(clrid: u32, error_code: u8) -> Vec<u8> {
    // Constant error frame (excluding CLRID)
    // magic(1) + clrid(4) + payload_len(4) + "ERROR" block
    const ERROR_PAYLOAD: &[u8] = &[
        5, // "ERROR" length
        b'E', b'R', b'R', b'O', b'R', // "ERROR"
        1, 0, // 1 field (u16)
        1, 0, // field id = 1 (u16)
        1, // field type = 1 (u8)
        4, 0, 0, 0, // field length = 4 (u32)
    ];

    // Error message depends on error_code
    let msg_bytes = match error_code {
        1 => b"404",
        _ => b"503",
    };

    // Payload: "ERROR" block + message
    let msg_len = msg_bytes.len();
    let payload_len = ERROR_PAYLOAD.len() + msg_len;
    let total_len = 1 + 4 + 4 + payload_len;

    let mut buf = Vec::with_capacity(total_len);

    // Magic
    buf.push(0xFF);

    // CLRID
    buf.extend_from_slice(&clrid.to_le_bytes());

    // Payload length
    buf.extend_from_slice(&(payload_len as u32).to_le_bytes());

    // "ERROR" block
    buf.extend_from_slice(ERROR_PAYLOAD);

    // Message length
    buf.extend_from_slice(&(msg_len as u32).to_le_bytes());

    // Message
    buf.extend_from_slice(msg_bytes);

    buf
}
