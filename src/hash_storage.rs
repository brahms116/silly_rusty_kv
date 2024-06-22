/// Extensible hashing storage
use tokio::fs::File;

use crate::command::*;

pub struct HashStorage {
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

impl HashStorage {
    /// Creates a represenation of the storage engine by specifying the directory and bucket files
    ///
    /// If the specified files do not exist, they will be created.
    ///
    /// # Arguments
    /// * `directory_file` - The file containing the directory
    /// * `buckets_file` - The file containing the buckets
    ///
    /// # Returns
    /// A new instance of the index
    pub fn new(directory_file: &str, buckets_file: &str) -> Self {
        let directory_file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(directory_file)
            .unwrap();

        let buckets_file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(buckets_file)
            .unwrap();

        todo!()
    }

    pub async fn handle_cmd(&mut self, cmd: Command) -> Result<(), ()> {
        match cmd {
            Command::Put(cmd) => self.put(cmd).await.unwrap(),
            Command::Delete(cmd) => self.delete(cmd).await.unwrap(),
            Command::Get(cmd) => {
                if let Some(value) = self.get(&cmd).await.unwrap() {
                    println!("{}", value);
                } else {
                    println!("Key not found");
                }
            }
            Command::Exit => {}
        }
        Ok(())
    }

    pub async fn put(&mut self, cmd: PutCommand) -> Result<(), ()> {
        todo!()
    }

    pub async fn get(&mut self, cmd: &GetCommand) -> Result<Option<String>, ()> {
        todo!()
    }

    pub async fn delete(&mut self, cmd: DeleteCommand) -> Result<(), ()> {
        todo!()
    }
}

/// Rust representation of a bucket
pub struct Bucket {
    /// The local level of the bucket
    level: u8,

    /// The records contained in the bucket
    records: Vec<PutCommand>,
}
