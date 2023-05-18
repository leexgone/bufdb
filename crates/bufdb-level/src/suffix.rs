use bufdb_api::error::ErrorKind;
use bufdb_api::error::Result;
use bufdb_storage::entry::BufferEntry;
use bufdb_storage::entry::Entry;
use bufdb_storage::entry::SliceEntry;
use bufdb_storage::io::BufferOutput;
use bufdb_storage::io::Output;

const SIGN_VALUE: u8 = u8::MAX - 8;

fn write_suffix(output: &mut BufferOutput, value: u32) -> Result<usize> {
    let len = if value <= SIGN_VALUE as _ {
        output.write_u8(value as _)?;
        1usize
    } else {
        let mut len = 0u8;
        let mut val = value - SIGN_VALUE as u32;
        while val > 0 {
            output.write_u8(val as _)?;
            val >>= 8;
            len += 1;
        }
        output.write_u8(SIGN_VALUE + len)?;
        len as usize + 1
    };

    Ok(len)
}

pub fn append_suffix(entry: BufferEntry, value: u32) -> Result<BufferEntry> {
    let mut output = {
        let off = entry.off();
        let pos = entry.len();
        BufferOutput::new_from_vec(entry.into(), off, pos)
    };

    write_suffix(&mut output, value)?;

    Ok(output.into())
}

pub fn reset_suffix(entry: BufferEntry, value: u32) -> Result<BufferEntry> {
    let mut output = {
        let off = entry.off();
        let pos = entry.len() - size_of_suffix(&entry);
        BufferOutput::new_from_vec(entry.into(), off, pos)
    };

    write_suffix(&mut output, value)?;

    Ok(output.into())
}

pub fn size_of_suffix<T: Entry>(entry: &T) -> usize {
    if entry.is_empty() {
        0
    } else {
        let buf: &[u8] = entry.as_ref();
        let n = buf[entry.len() - 1];
        if n <= SIGN_VALUE {
            1
        } else {
            (n - SIGN_VALUE + 1) as usize
        }
    }
}

pub fn unwrap_suffix(buf: &BufferEntry) -> Result<(SliceEntry, u32)> {
    let mut iter = buf.slice().iter().rev();

    let sign = if let Some(&n) = iter.next() {
        n
    } else {
        return Err(ErrorKind::OutOfBounds.into());
    };

    let (val, len) = if sign <= SIGN_VALUE {
        (sign as u32, 1usize)
    } else {
        let len = (sign - SIGN_VALUE) as usize;
        let mut val = 0u32;
        for _ in 0..len {
            if let Some(n) = iter.next() {
                val = (val << 8) + *n as u32;
            } else {
                return Err(ErrorKind::OutOfBounds.into());
            }
        }

        (val + SIGN_VALUE as u32, len + 1)
    };

    let slice = buf.left(buf.size() - len)?;

    Ok((slice, val))
}

#[cfg(test)]
mod tests {
    use bufdb_storage::entry::BufferEntry;
    use bufdb_storage::entry::Entry;

    use crate::suffix::reset_suffix;
    use crate::suffix::size_of_suffix;

    use super::SIGN_VALUE;
    use super::append_suffix;
    use super::unwrap_suffix;

    fn check(value: u32, len: Option<usize>) {
        let buf = BufferEntry::default();
        let buf = append_suffix(buf, value).unwrap();
        let len = if let Some(n) = len {
            assert_eq!(n, buf.size());
            n
        } else {
            buf.size()
        };
        let (raw, n) = unwrap_suffix(&buf).unwrap();
        assert!(raw.is_empty());
        assert_eq!(value, n);
        assert_eq!(len, size_of_suffix(&buf));

        let buf = BufferEntry::from(vec![1u8, 2u8, 3u8]);
        assert_eq!(3, buf.size());
        let buf = append_suffix(buf, value).unwrap();
        assert_eq!(len + 3, buf.size());
        let (raw, n) = unwrap_suffix(&buf).unwrap();
        assert_eq!(&[1u8, 2u8, 3u8], raw.slice());
        assert_eq!(value, n);
        assert_eq!(len, size_of_suffix(&buf));

        let next = if value == u32::MAX {
            value - 1
        } else {
            value + 1
        };
        let buf = reset_suffix(buf, next).unwrap();
        let (_, n) = unwrap_suffix(&buf).unwrap();
        assert_eq!(next, n);
    }

    #[test]
    fn test_suffix() {
        check(0, Some(1));
        check(1, Some(1));
        check(SIGN_VALUE as _, Some(1));
        check(u8::MAX as _, Some(2));
        check(u32::MAX, Some(5));

        check(247 + 0x00000000u32, Some(1));
        check(247 + 0x00000001u32, Some(2));
        check(247 + 0x00000100u32, Some(3));
        check(247 + 0x00010000u32, Some(4));
        check(247 + 0x01000000u32, Some(5));

        check(12345676, None);
        check(0xFA09E500, None);
    }
}