use bufdb_storage::Cursor;
use bufdb_storage::SecondaryCursor;

#[derive(Debug)]
pub struct PKCursor {

}

impl Cursor for PKCursor {
    fn search(&self, key: &bufdb_storage::entry::BufferEntry, data: Option<&mut bufdb_storage::entry::BufferEntry>) -> bufdb_api::error::Result<bool> {
        todo!()
    }

    fn search_range(&self, key: &mut bufdb_storage::entry::BufferEntry, data: Option<&mut bufdb_storage::entry::BufferEntry>) -> bufdb_api::error::Result<bool> {
        todo!()
    }

    fn next(&self, key: &mut bufdb_storage::entry::BufferEntry, data: &mut bufdb_storage::entry::BufferEntry) -> bufdb_api::error::Result<bool> {
        todo!()
    }

    fn next_dup(&self, key: &mut bufdb_storage::entry::BufferEntry, data: &mut bufdb_storage::entry::BufferEntry) -> bufdb_api::error::Result<bool> {
        todo!()
    }

    fn skip(&self, count: usize, key: Option<&mut bufdb_storage::entry::BufferEntry>, data: Option<&mut bufdb_storage::entry::BufferEntry>) -> bufdb_api::error::Result<bool> {
        todo!()
    }

    fn delete(&mut self, key: &bufdb_storage::entry::BufferEntry) -> bufdb_api::error::Result<bool> {
        todo!()
    }

    fn update(&mut self, key: &bufdb_storage::entry::BufferEntry, data: &bufdb_storage::entry::BufferEntry) -> bufdb_api::error::Result<bool> {
        todo!()
    }
}

#[derive(Debug)]
pub struct IDXCursor {

}

impl Cursor for IDXCursor {
    fn search(&self, key: &bufdb_storage::entry::BufferEntry, data: Option<&mut bufdb_storage::entry::BufferEntry>) -> bufdb_api::error::Result<bool> {
        todo!()
    }

    fn search_range(&self, key: &mut bufdb_storage::entry::BufferEntry, data: Option<&mut bufdb_storage::entry::BufferEntry>) -> bufdb_api::error::Result<bool> {
        todo!()
    }

    fn next(&self, key: &mut bufdb_storage::entry::BufferEntry, data: &mut bufdb_storage::entry::BufferEntry) -> bufdb_api::error::Result<bool> {
        todo!()
    }

    fn next_dup(&self, key: &mut bufdb_storage::entry::BufferEntry, data: &mut bufdb_storage::entry::BufferEntry) -> bufdb_api::error::Result<bool> {
        todo!()
    }

    fn skip(&self, count: usize, key: Option<&mut bufdb_storage::entry::BufferEntry>, data: Option<&mut bufdb_storage::entry::BufferEntry>) -> bufdb_api::error::Result<bool> {
        todo!()
    }

    fn delete(&mut self, key: &bufdb_storage::entry::BufferEntry) -> bufdb_api::error::Result<bool> {
        todo!()
    }

    fn update(&mut self, key: &bufdb_storage::entry::BufferEntry, data: &bufdb_storage::entry::BufferEntry) -> bufdb_api::error::Result<bool> {
        todo!()
    }
}

impl SecondaryCursor for IDXCursor {
    fn s_search(&self, key: &bufdb_storage::entry::BufferEntry, p_key: Option<&mut bufdb_storage::entry::BufferEntry>, data: Option<&mut bufdb_storage::entry::BufferEntry>) -> bufdb_api::error::Result<bool> {
        todo!()
    }

    fn s_search_range(&self, key: &mut bufdb_storage::entry::BufferEntry, p_key: Option<&mut bufdb_storage::entry::BufferEntry>, data: Option<&mut bufdb_storage::entry::BufferEntry>) -> bufdb_api::error::Result<bool> {
        todo!()
    }

    fn s_next(&self, key: &mut bufdb_storage::entry::BufferEntry, p_key: Option<&mut bufdb_storage::entry::BufferEntry>, data: Option<&mut bufdb_storage::entry::BufferEntry>) -> bufdb_api::error::Result<bool> {
        todo!()
    }

    fn s_next_dup(&self, key: &mut bufdb_storage::entry::BufferEntry, p_key: Option<&mut bufdb_storage::entry::BufferEntry>, data: Option<&mut bufdb_storage::entry::BufferEntry>) -> bufdb_api::error::Result<bool> {
        todo!()
    }
}