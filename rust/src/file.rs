use std::io::{self, Cursor, Read, Seek, SeekFrom, Write};

use fsspec_rs::{FileInfo, FsFile, FsResult};

pub struct DbFile {
    cursor: Cursor<Vec<u8>>,
    info: FileInfo,
    writable: bool,
}

impl DbFile {
    pub fn readable(data: Vec<u8>, info: FileInfo) -> Self {
        Self {
            cursor: Cursor::new(data),
            info,
            writable: false,
        }
    }
}

impl Read for DbFile {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.cursor.read(buf)
    }
}

impl Write for DbFile {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if !self.writable {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "database file is read-only",
            ));
        }
        self.cursor.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl Seek for DbFile {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.cursor.seek(pos)
    }
}

impl FsFile for DbFile {
    fn info(&self) -> FsResult<FileInfo> {
        Ok(self.info.clone())
    }

    fn size(&self) -> FsResult<Option<u64>> {
        Ok(Some(self.cursor.get_ref().len() as u64))
    }
}
