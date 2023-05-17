use bufdb_api::error::Result;
use bufdb_storage::entry::BufferEntry;
use bufdb_storage::entry::SliceEntry;

pub fn append_suffix(entry: BufferEntry, value: u32) -> Result<BufferEntry> {
    todo!()
}

pub fn unwrap_suffix(buf: &BufferEntry) -> Result<(SliceEntry, u32)> {
    todo!()
}