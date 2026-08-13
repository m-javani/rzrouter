use crate::error::RZError;

pub const ROUTER_MAGIC: u8 = 0xFE;
pub const SHARD_MAGIC: u8 = 0xFF;

/// Special control segment used for application-level keepalives.
pub const KEEPALIVE_SEGMENT: &str = "__keepalive__";

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
/// Returns `Err` only for *unrecoverable* structural problems that should
/// trigger a resync attempt in the caller.
pub fn try_decode_router(buf: &[u8]) -> Result<Option<RouterFrame<'_>>, RZError> {
    if buf.len() < 5 {
        return Ok(None);
    }

    if buf[0] != ROUTER_MAGIC {
        return Err(RZError::ParseError("bad router magic".into()));
    }

    let total_len = u32::from_le_bytes(buf[1..5].try_into().unwrap()) as usize;
    let frame_len = 5 + total_len;

    // Sanity: reject absurdly large frames early
    if total_len > 16 * 1024 * 1024 {
        return Err(RZError::ParseError("frame too large".into()));
    }

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
///
/// Error codes:
/// 1 = unknown segment
/// 2 = network error
/// 3 = timeout
/// 4 = internal error
pub fn build_error_response(clrid: u32, error_code: u8) -> Vec<u8> {
    const ERROR_PAYLOAD: &[u8] = &[
        5, // "ERROR" length
        b'E', b'R', b'R', b'O', b'R', // "ERROR"
        1, 0, // 1 field (u16)
        1, 0, // field id = 1 (u16)
        1, // field type = 1 (u8)
        4, 0, 0, 0, // field length = 4 (u32)
    ];

    let msg_bytes = match error_code {
        1 => b"404",
        2 => b"503",
        3 => b"408", // timeout
        4 => b"500", // internal
        _ => b"500",
    };

    let msg_len = msg_bytes.len();
    let payload_len = ERROR_PAYLOAD.len() + 4 + msg_len; // +4 for msg_len u32

    let mut buf = Vec::with_capacity(1 + 4 + 4 + payload_len);
    buf.push(0xFF); // shard magic
    buf.extend_from_slice(&clrid.to_le_bytes());
    buf.extend_from_slice(&(payload_len as u32).to_le_bytes());
    buf.extend_from_slice(ERROR_PAYLOAD);
    buf.extend_from_slice(&(msg_len as u32).to_le_bytes());
    buf.extend_from_slice(msg_bytes);
    buf
}

/// Build an application-level keepalive (ping) frame.
/// Uses the special segment `__keepalive__` with empty payload and is_write=0.
pub fn build_keepalive_frame(clrid: u32) -> Vec<u8> {
    // Layout:
    // ROUTER_MAGIC (1)
    // total_len (4)  = 1 (seg_len) + seg + 1 (is_write) + 9 (shard header)
    // seg_len (1)
    // segment bytes
    // is_write (1)
    // SHARD_MAGIC (1)
    // clrid (4)
    // remaining shard payload (empty for keepalive)

    let segment = KEEPALIVE_SEGMENT.as_bytes();
    let seg_len = segment.len() as u8;
    let total_len = 1 + seg_len as usize + 1 + 9; // +9 for shard magic+clrid+placeholder

    let mut buf = Vec::with_capacity(5 + total_len);
    buf.push(ROUTER_MAGIC);
    buf.extend_from_slice(&(total_len as u32).to_le_bytes());
    buf.push(seg_len);
    buf.extend_from_slice(segment);
    buf.push(0x00); // is_write = false
    buf.push(SHARD_MAGIC);
    buf.extend_from_slice(&clrid.to_le_bytes());
    // empty remaining payload
    buf.extend_from_slice(&[0u8; 4]); // just to keep shard header size consistent if needed
    buf
}

/// Find the next occurrence of ROUTER_MAGIC starting from `from`.
/// Returns the absolute index or None.
pub fn find_next_router_magic(buf: &[u8], from: usize) -> Option<usize> {
    buf[from..]
        .iter()
        .position(|&b| b == ROUTER_MAGIC)
        .map(|p| from + p)
}
