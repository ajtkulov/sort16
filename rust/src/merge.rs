use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crate::record::{self, RECORD_SIZE};

struct RunReader {
    path: PathBuf,
    file_offset: u64,
    size: u64,
    buffer_size: usize,
    #[allow(dead_code)]
    index: usize,
    buffer: Vec<u8>,
    record_index: usize,
    record_count: usize,
}

impl RunReader {
    fn new(path: impl AsRef<Path>, file_offset: u64, buffer_size: usize, index: usize) -> std::io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        let size = std::fs::metadata(&path)?.len();
        Ok(Self {
            path,
            file_offset,
            size,
            buffer_size,
            index,
            buffer: Vec::new(),
            record_index: 0,
            record_count: 0,
        })
    }

    fn has_current(&self) -> bool {
        self.record_index < self.record_count
    }

    fn current_record(&self) -> [u8; RECORD_SIZE] {
        let off = self.record_index * RECORD_SIZE;
        let mut rec = [0u8; RECORD_SIZE];
        rec.copy_from_slice(&self.buffer[off..off + RECORD_SIZE]);
        rec
    }

    fn load(&mut self) -> std::io::Result<bool> {
        if self.file_offset >= self.size {
            self.buffer.clear();
            self.record_index = 0;
            self.record_count = 0;
            return Ok(false);
        }
        let mut file = File::open(&self.path)?;
        file.seek(SeekFrom::Start(self.file_offset))?;
        let mut buffer = vec![0u8; self.buffer_size];
        let n = file.read(&mut buffer)?;
        assert!(
            n % RECORD_SIZE == 0,
            "bytes_read {} not divisible by 16",
            n
        );
        buffer.truncate(n);
        self.record_count = n / RECORD_SIZE;
        self.record_index = 0;
        self.file_offset += n as u64;
        self.buffer = buffer;
        Ok(self.record_count > 0)
    }

    fn advance(&mut self) {
        self.record_index += 1;
    }
}

#[derive(Eq, PartialEq)]
struct HeapItem {
    key: [u8; RECORD_SIZE],
    run_index: usize, // run id
}

impl Ord for HeapItem {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Natural ascending order (used under Reverse for min-heap via BinaryHeap).
        record::compare_records(&self.key, &other.key)
    }
}

impl PartialOrd for HeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

pub fn merge_runs(
    sorted_files: &[PathBuf],
    output_file_name: impl AsRef<Path>,
    read_buffer_size: usize,
) -> std::io::Result<()> {
    let out = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(output_file_name.as_ref())?;
    let mut output = BufWriter::with_capacity(10 * 1024 * 1024, out);

    let mut readers: Vec<RunReader> = Vec::with_capacity(sorted_files.len());
    let mut heap: BinaryHeap<Reverse<HeapItem>> = BinaryHeap::new();

    for (idx, f) in sorted_files.iter().enumerate() {
        let mut reader = RunReader::new(f, 0, read_buffer_size, idx)?;
        if reader.load()? {
            heap.push(Reverse(HeapItem {
                key: reader.current_record(),
                run_index: idx,
            }));
        }
        readers.push(reader);
    }

    while let Some(Reverse(head)) = heap.pop() {
        output.write_all(&head.key)?;
        let reader = &mut readers[head.run_index];
        reader.advance();
        if reader.has_current() {
            heap.push(Reverse(HeapItem {
                key: reader.current_record(),
                run_index: head.run_index,
            }));
        } else if reader.load()? {
            heap.push(Reverse(HeapItem {
                key: reader.current_record(),
                run_index: head.run_index,
            }));
        }
    }

    output.flush()?;
    Ok(())
}

/// Exposed for invalid-length tests.
pub fn try_load_run(path: impl AsRef<Path>, buffer_size: usize) -> std::io::Result<()> {
    let mut reader = RunReader::new(path, 0, buffer_size, 0)?;
    let _ = reader.load()?;
    Ok(())
}
