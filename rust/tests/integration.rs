use std::fs;
use std::io::Write;
use std::path::Path;

use sort16::merge::{merge_runs, try_load_run};
use sort16::record::{compare, pack, unpack, RECORD_SIZE};
use sort16::sort::{clean_up, run_sort, sort_file};
use sort16::batch::Batch;
use tempfile::tempdir;

type Rec = (i32, i32, i32, i32);

fn write_dat(path: &Path, records: &[Rec]) {
    let mut f = fs::File::create(path).unwrap();
    for &(a, b, c, d) in records {
        f.write_all(&pack(a, b, c, d)).unwrap();
    }
}

fn read_dat(path: &Path) -> Vec<Rec> {
    let bytes = fs::read(path).unwrap();
    assert_eq!(bytes.len() % RECORD_SIZE, 0);
    bytes
        .chunks_exact(RECORD_SIZE)
        .map(|ch| unpack(ch, 0))
        .collect()
}

fn is_non_decreasing(records: &[Rec]) -> bool {
    records.windows(2).all(|w| {
        let a = pack(w[0].0, w[0].1, w[0].2, w[0].3);
        let b = pack(w[1].0, w[1].1, w[1].2, w[1].3);
        compare(&a, 0, &b, 0) != std::cmp::Ordering::Greater
    })
}

#[test]
fn orders_by_each_integer_position() {
    assert!(compare(&pack(1, 0, 0, 0), 0, &pack(2, 0, 0, 0), 0).is_lt());
    assert!(compare(&pack(0, 1, 9, 9), 0, &pack(0, 2, 0, 0), 0).is_lt());
    assert!(compare(&pack(0, 0, 1, 9), 0, &pack(0, 0, 2, 0), 0).is_lt());
    assert!(compare(&pack(0, 0, 0, 1), 0, &pack(0, 0, 0, 2), 0).is_lt());
}

#[test]
fn equal_records_compare_equal() {
    assert!(compare(&pack(1, 2, 3, 4), 0, &pack(1, 2, 3, 4), 0).is_eq());
}

#[test]
fn signed_high_bit_keys() {
    assert!(compare(&pack(-1, 0, 0, 0), 0, &pack(0, 0, 0, 0), 0).is_lt());
    assert!(compare(&pack(i32::MIN, 0, 0, 0), 0, &pack(i32::MAX, 0, 0, 0), 0).is_lt());
}

#[test]
fn single_block_sort_end_to_end() {
    let dir = tempdir().unwrap();
    let input = dir.path().join("in.dat");
    let output = dir.path().join("out.dat");
    let records = vec![(3, 0, 0, 0), (1, 0, 0, 0), (2, 0, 0, 1), (2, 0, 0, 0)];
    write_dat(&input, &records);
    run_sort(&[input.clone()], &output, 64, 1, 16, "sort").unwrap();
    let sorted = read_dat(&output);
    assert_eq!(sorted.len(), records.len());
    assert!(is_non_decreasing(&sorted));
    let mut expected = records.clone();
    expected.sort();
    assert_eq!(sorted, expected);
}

#[test]
fn multi_block_tiny_blocksize() {
    let dir = tempdir().unwrap();
    let input = dir.path().join("in.dat");
    let output = dir.path().join("out.dat");
    let records = vec![
        (5, 0, 0, 0),
        (1, 0, 0, 0),
        (4, 0, 0, 0),
        (2, 0, 0, 0),
        (6, 0, 0, 0),
        (3, 0, 0, 0),
    ];
    write_dat(&input, &records);
    let runs = sort_file(&[input.clone()], dir.path().join("unused.tmp"), 32, 2).unwrap();
    assert_eq!(runs.len(), 3);
    assert!(runs[0].to_string_lossy().ends_with("in.dat.0"));
    merge_runs(&runs, &output, 16).unwrap();
    clean_up(&runs);
    let sorted = read_dat(&output);
    let mut expected = records;
    expected.sort();
    assert_eq!(sorted, expected);
    for r in &runs {
        assert!(!r.exists());
    }
}

#[test]
fn multi_file_inputs() {
    let dir = tempdir().unwrap();
    let a = dir.path().join("a.dat");
    let b = dir.path().join("b.dat");
    let output = dir.path().join("out.dat");
    write_dat(&a, &[(9, 0, 0, 0), (1, 0, 0, 0)]);
    write_dat(&b, &[(5, 0, 0, 0), (3, 0, 0, 0), (7, 0, 0, 0)]);
    let before_a = fs::read(&a).unwrap();
    let before_b = fs::read(&b).unwrap();
    run_sort(&[a.clone(), b.clone()], &output, 32, 2, 16, "sort").unwrap();
    assert_eq!(fs::read(&a).unwrap(), before_a);
    assert_eq!(fs::read(&b).unwrap(), before_b);
    let sorted = read_dat(&output);
    assert_eq!(sorted.len(), 5);
    assert!(is_non_decreasing(&sorted));
}

#[test]
fn merge_sorted_runs_and_tiny_refill() {
    let dir = tempdir().unwrap();
    let r1 = dir.path().join("r1.dat");
    let r2 = dir.path().join("r2.dat");
    let output = dir.path().join("out.dat");
    write_dat(&r1, &[(1, 0, 0, 0), (4, 0, 0, 0), (7, 0, 0, 0)]);
    write_dat(&r2, &[(2, 0, 0, 0), (3, 0, 0, 0), (9, 0, 0, 0)]);
    merge_runs(&[r1, r2], &output, 48).unwrap();
    assert_eq!(
        read_dat(&output),
        vec![
            (1, 0, 0, 0),
            (2, 0, 0, 0),
            (3, 0, 0, 0),
            (4, 0, 0, 0),
            (7, 0, 0, 0),
            (9, 0, 0, 0)
        ]
    );

    let run = dir.path().join("r.dat");
    let out2 = dir.path().join("out2.dat");
    write_dat(&run, &[(1, 0, 0, 0), (2, 0, 0, 0), (3, 0, 0, 0), (4, 0, 0, 0)]);
    merge_runs(&[run], &out2, 16).unwrap();
    assert_eq!(
        read_dat(&out2),
        vec![(1, 0, 0, 0), (2, 0, 0, 0), (3, 0, 0, 0), (4, 0, 0, 0)]
    );
}

#[test]
fn merge_only_skips_batch_partition() {
    let dir = tempdir().unwrap();
    let a = dir.path().join("a.dat");
    let b = dir.path().join("b.dat");
    let output = dir.path().join("out.dat");
    write_dat(&a, &[(1, 0, 0, 0), (5, 0, 0, 0)]);
    write_dat(&b, &[(2, 0, 0, 0), (3, 0, 0, 0)]);
    run_sort(&[a, b], &output, 32, 1, 16, "merge").unwrap();
    assert_eq!(
        read_dat(&output),
        vec![(1, 0, 0, 0), (2, 0, 0, 0), (3, 0, 0, 0), (5, 0, 0, 0)]
    );
}

#[test]
fn invalid_length_batch_and_run_reader() {
    let dir = tempdir().unwrap();
    let bad = dir.path().join("bad.dat");
    fs::write(&bad, [1u8, 2, 3, 4, 5]).unwrap();
    let batch = Batch::new(&bad, 0, &bad, 0, 64);
    let err = std::panic::catch_unwind(|| batch.pipeline().unwrap());
    assert!(err.is_err());

    let bad2 = dir.path().join("bad2.dat");
    fs::write(&bad2, [1u8; 15]).unwrap();
    let err2 = std::panic::catch_unwind(|| try_load_run(&bad2, 64).unwrap());
    assert!(err2.is_err());
}

#[test]
fn cli_defaults_parse() {
    use clap::Parser;
    use sort16::cli::Args;
    let args = Args::parse_from(["sort16", "--output", "out.dat", "in.dat"]);
    assert_eq!(args.blocksize, 1_000_000_000);
    assert_eq!(args.threads, 12);
    assert_eq!(args.readbuffersize, 20_000_000);
    assert_eq!(args.action, "sort");
}
