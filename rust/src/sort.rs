use std::fs;
use std::path::{Path, PathBuf};

use rayon::prelude::*;

use crate::batch::Batch;
use crate::merge;

pub fn sort_file(
    files: &[PathBuf],
    _output_tmp_hint: impl AsRef<Path>,
    block_size: usize,
    max_concurrency: usize,
) -> std::io::Result<Vec<PathBuf>> {
    let mut specs: Vec<(PathBuf, u64)> = Vec::new();
    for file_name in files {
        let size = fs::metadata(file_name)?.len();
        // Match Scala: `0 to ((size - 1) / blockSize)` even for empty files.
        let last = if size == 0 {
            0
        } else {
            ((size - 1) / block_size as u64) as usize
        };
        for block_idx in 0..=last {
            specs.push((file_name.clone(), block_size as u64 * block_idx as u64));
        }
    }

    let batches: Vec<Batch> = specs
        .into_iter()
        .enumerate()
        .map(|(idx, (file_name, offset))| {
            Batch::new(file_name.clone(), offset, file_name, idx, block_size)
        })
        .collect();

    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(max_concurrency.max(1))
        .build()
        .map_err(|e| std::io::Error::other(e.to_string()))?;

    pool.install(|| {
        batches.par_iter().try_for_each(|b| b.pipeline())?;
        Ok::<(), std::io::Error>(())
    })?;

    Ok(batches.iter().map(|b| b.output_file()).collect())
}

pub fn clean_up(files: &[PathBuf]) {
    for f in files {
        let _ = fs::remove_file(f);
    }
}

pub fn run_sort(
    files: &[PathBuf],
    output: &Path,
    block_size: usize,
    threads: usize,
    read_buffer_size: usize,
    action: &str,
) -> std::io::Result<()> {
    if action == "sort" {
        let runs = sort_file(files, format!("{}.tmp", output.display()), block_size, threads)?;
        merge::merge_runs(&runs, output, read_buffer_size)?;
        clean_up(&runs);
    } else {
        merge::merge_runs(files, output, read_buffer_size)?;
    }
    Ok(())
}
