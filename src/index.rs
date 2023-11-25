/// An attempt using extendible hashing to provide an index for the database.
use tokio::fs::File;

/// A rust representation of the index.
///
/// The index is represented by two files:
/// 1. A lookup table, which maps a hash to a bucket address
/// 2. A bucket, which contains a list of records
pub struct Index {
    /// The file containing the look up table and the global level
    lookup_file: File,

    /// The file containing the buckets
    buckets_file: File,

    /// The global level of the index
    global_level: u8,

    /// A lookup table mapping a hash to a bucket address
    lookup_table: Vec<LookupTableRow>,
}

/// A rust representation of a row in the lookup table
pub struct LookupTableRow {
    /// A matching hash. If the hash of a key matches the first n bits of this hash, then
    /// bucket_addr points to the bucket containing the record with that key. n here is the global
    /// level of the [Index].
    matching_hash: u64,

    /// The address of the bucket which contains the records which match the matching_hash. See
    /// [LookupTableRow] for more information.
    bucket_addr: u64,
}

/// Rust representation of a bucket
pub struct Bucket {
    /// The local level of the bucket
    level: u8,

    /// The records contained in the bucket
    records: Vec<IndexRecord>,
}

pub struct IndexRecord {
    hashed_key: u64,
    data_file_addr: u64,
}
