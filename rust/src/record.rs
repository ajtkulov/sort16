//! Record format: 16-byte records ordered as four signed big-endian i32s.

use std::cmp::Ordering;

pub const RECORD_SIZE: usize = 16;

#[inline]
pub fn read_int_be(bytes: &[u8], offset: usize) -> i32 {
    i32::from_be_bytes([
        bytes[offset],
        bytes[offset + 1],
        bytes[offset + 2],
        bytes[offset + 3],
    ])
}

/// Natural order over four signed big-endian ints.
#[inline]
pub fn compare(left: &[u8], left_offset: usize, right: &[u8], right_offset: usize) -> Ordering {
    let mut i = 0;
    while i < 4 {
        let l = read_int_be(left, left_offset + i * 4);
        let r = read_int_be(right, right_offset + i * 4);
        if l != r {
            return l.cmp(&r);
        }
        i += 1;
    }
    Ordering::Equal
}

pub fn compare_records(a: &[u8; RECORD_SIZE], b: &[u8; RECORD_SIZE]) -> Ordering {
    compare(a, 0, b, 0)
}

pub fn pack(i0: i32, i1: i32, i2: i32, i3: i32) -> [u8; RECORD_SIZE] {
    let mut out = [0u8; RECORD_SIZE];
    out[0..4].copy_from_slice(&i0.to_be_bytes());
    out[4..8].copy_from_slice(&i1.to_be_bytes());
    out[8..12].copy_from_slice(&i2.to_be_bytes());
    out[12..16].copy_from_slice(&i3.to_be_bytes());
    out
}

pub fn unpack(bytes: &[u8], offset: usize) -> (i32, i32, i32, i32) {
    (
        read_int_be(bytes, offset),
        read_int_be(bytes, offset + 4),
        read_int_be(bytes, offset + 8),
        read_int_be(bytes, offset + 12),
    )
}
