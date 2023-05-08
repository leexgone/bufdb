#[derive(Debug, Default, Clone)]
pub struct BufferEntry {
    data: Vec<u8>,
    offset: usize,
    len: usize
}

impl BufferEntry {
    pub fn new<T: Into<Vec<u8>>>(data: T, offset: usize, len: usize) -> BufferEntry {
        BufferEntry { 
            data: data.into(), 
            offset, 
            len 
        }
    }

    pub fn offset(&self) -> usize {
        self.offset
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn set_data(&mut self, data: Vec<u8>) {
        self.offset = 0;
        self.len = data.len();
        self.data = data;
    }

    pub fn set_data_offset(&mut self, data: Vec<u8>, offset: usize, len: usize) {
        self.offset = offset;
        self.len = len;
        self.data = data;
    }
}

impl AsRef<Vec<u8>> for BufferEntry {
    fn as_ref(&self) -> &Vec<u8> {
        &self.data
    }
}

impl AsRef<[u8]> for BufferEntry {
    fn as_ref(&self) -> &[u8] {
        &self.data
    }
}

impl AsMut<[u8]> for BufferEntry {
    fn as_mut(&mut self) -> &mut [u8] {
        &mut self.data
    }
}

impl <T: Into<Vec<u8>>> From<T> for BufferEntry {
    fn from(value: T) -> Self {
        let data: Vec<u8> = value.into();
        let len = data.len();
        BufferEntry { 
            data, 
            offset: 0, 
            len
        }
    }
}
