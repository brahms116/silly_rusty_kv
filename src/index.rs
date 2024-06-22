/// An attempt using extendible hashing to provide an index for the database.
use tokio::fs::File;

/// A rust representation of the index.
///
/// The index is represented by two physical disk files:
/// 1. A directory, which maps a hash to a bucket address
/// 2. A bucket, which contains a list of records
pub struct Index {
    /// The file containing the look up table and the global level
    directory_file: File,

    /// The file containing the buckets
    buckets_file: File,

    /// The global level of the index
    global_level: u8,

    /// Pointers to the buckets
    ///
    /// Together with the global_level, this is the directory of the index.
    /// The vector is sorted by the hash codes of the buckets.
    ///
    /// For example if the global level is 3, the directory will look like this:
    /// 000 -> element 0
    /// 001 -> element 1
    /// 010 -> element 2
    /// 011 -> element 3
    /// 100 -> element 4
    /// 101 -> element 5
    /// ... and so on
    bucket_addresses: Vec<u64>,
}

impl Index {
    /// Creates a represenation of the index by specifying the directory and bucket files
    ///
    /// If the specified files do not exist, they will be created.
    ///
    /// # Arguments
    /// * `directory_file` - The file containing the directory
    /// * `buckets_file` - The file containing the buckets
    ///
    /// # Returns
    /// A new instance of the index
    pub fn new(directory_file: &str, buckets_file: &str) -> Index {
        todo!()
    }
}

/// Rust representation of a bucket
pub struct Bucket {
    /// The local level of the bucket
    level: u8,

    /// The records contained in the bucket
    records: Vec<IndexRecord>,
}

/// Rust representation of a record in the index
///
/// The record associates a storage record's hashed key with its address inside the storage file
pub struct IndexRecord {
    /// The hashed key of the storage record
    hashed_storage_record_key: u64,

    /// The address of the storage record inside the storage file
    storage_record_addr: u64,
}

