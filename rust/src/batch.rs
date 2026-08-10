use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crate::record::{self, RECORD_SIZE};

pub struct Batch {
    input_path: PathBuf,
    offset: u64,
    output_file_name: PathBuf,
    pub idx: usize,
    block_size: usize,
}

impl Batch {
    pub fn new(
        input_path: impl AsRef<Path>,
        offset: u64,
        output_file_name: impl AsRef<Path>,
        idx: usize,
        block_size: usize,
    ) -> Self {
        Self {
            input_path: input_path.as_ref().to_path_buf(),
            offset,
            output_file_name: output_file_name.as_ref().to_path_buf(),
            idx,
            block_size,
        }
    }

    pub fn output_file(&self) -> PathBuf {
        PathBuf::from(format!("{}.{}", self.output_file_name.display(), self.idx))
    }

    pub fn pipeline(&self) -> std::io::Result<()> {
        let mut file = File::open(&self.input_path)?;
        file.seek(SeekFrom::Start(self.offset))?;
        let mut buffer = vec![0u8; self.block_size];
        let bytes_read = file.read(&mut buffer)?;
        assert!(
            bytes_read % RECORD_SIZE == 0,
            "bytes_read {} not divisible by 16",
            bytes_read
        );
        buffer.truncate(bytes_read);
        let items_count = bytes_read / RECORD_SIZE;

        let mut indices: Vec<usize> = (0..items_count).collect();
        indices.sort_by(|&l, &r| {
            record::compare(&buffer, l * RECORD_SIZE, &buffer, r * RECORD_SIZE)
        });

        let mut sorted = vec![0u8; items_count * RECORD_SIZE];
        for (i, &idx) in indices.iter().enumerate() {
            let src = idx * RECORD_SIZE;
            let dst = i * RECORD_SIZE;
            sorted[dst..dst + RECORD_SIZE].copy_from_slice(&buffer[src..src + RECORD_SIZE]);
        }

        let out_path = self.output_file();
        let mut out = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&out_path)?;
        out.write_all(&sorted)?;
        Ok(())
    }
}
