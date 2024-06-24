/// Extensible hashing storage
/// TODO: Use the constants instead of weird having random numbers everywhere
use crate::consts::*;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::io::SeekFrom;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

use crate::command::*;

fn hash_string_key(key: &str) -> u64 {
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    hasher.finish()
}

/// Given an length of the directory addresses derive the
/// global level
///
/// This should just be log base 2 of the global level or
/// the position of most significant bit
fn addr_count_to_global_level(mut length: usize) -> u8 {
    let mut result: u8 = 0;
    while length > 0 {
        length = length >> 1;
        result += 1;
    }
    return result;
}

async fn load_bucket_file(file: &mut File) -> u32 {
    if file.metadata().await.unwrap().len() == 0 {
        // setup the file by pushing an empty bucket to it
        let bucket = Bucket {
            records: vec![],
            level: 0,
            bucket_index: 0,
            // Doesn't matter
            remaining_byte_space: 0,
        };
        bucket.save_to_file(file).await;
        return 1;
    }
    file.seek(SeekFrom::Start(0)).await.unwrap();
    return file.read_u32_le().await.unwrap();
}

async fn save_bucket_file(bucket_count: u32, file: &mut File) {
    file.seek(SeekFrom::Start(0)).await.unwrap();
    let buf = bucket_count.to_le_bytes();
    return file.write_all(&buf).await.unwrap();
}

async fn load_directory(file: &mut File) -> (Vec<u32>, u8) {
    // Return if the file is empty
    if file.metadata().await.unwrap().len() == 0 {
        return (vec![0], 0);
    }

    file.seek(SeekFrom::Start(0)).await.unwrap();
    let global_level = file.read_u8().await.unwrap();
    let addr_count = (2 as usize).pow(global_level.into());
    let mut buf = vec![0; addr_count * 8];
    file.read_exact(&mut buf).await.unwrap();

    let mut result = vec![0; addr_count];
    for i in 0..addr_count {
        let start = 8 * i;
        result.push(u32::from_le_bytes([
            buf[start],
            buf[start + 1],
            buf[start + 2],
            buf[start + 3],
        ]));
    }
    return (result, global_level);
}

async fn save_directory(vec: &Vec<u32>, file: &mut File) {
    let addr_count = vec.len();
    let global_level = addr_count_to_global_level(addr_count);
    file.seek(SeekFrom::Start(0)).await.unwrap();
    file.write_u8(global_level).await.unwrap();
    let mut buf = vec![0; vec.len() * 8];
    for i in 0..addr_count {
        let bytes = vec[i].to_le_bytes();
        let start = 8 * i;
        buf[start] = bytes[0];
        buf[start + 1] = bytes[1];
        buf[start + 2] = bytes[2];
        buf[start + 3] = bytes[3];
    }

    file.write_all(&buf).await.unwrap();
}

/// Hash storage engine
///
/// Abitarily chosen max bucket number to be u64::MAX the
pub struct HashStorage {
    /// The file containing the look up table and the global level
    ///
    /// ## File layout
    /// - First byte is the global level
    /// - Next is followed by the list of u32s stored in LE
    /// The length of this list is 2^global_level
    ///
    /// There are no pages in this file, the entire file is loaded and saved all at once
    directory_file: File,

    /// The file containing the buckets
    ///
    /// ## File layout
    /// - First u32 is the number of current buckets
    /// - Followed by pages of PAGE_SIZE, with each page being a bucket
    buckets_file: File,

    /// The current number of buckets, we need this to know
    /// where to create new buckets, loaded from the buckets file
    bucket_count: u32,

    /// The global level of the index
    ///
    /// Saved and loaded from the directory file
    global_level: u8,

    /// Pointers to the bucket number
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
    ///
    /// This is loaded and saved from the directory file
    bucket_lookup: Vec<u32>,
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
    pub async fn new(directory_file: &str, buckets_file: &str) -> Self {
        let mut directory_file = std::fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(directory_file)
            .unwrap()
            .into();

        let mut buckets_file = std::fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(buckets_file)
            .unwrap()
            .into();

        let (bucket_addresses, global_level) = load_directory(&mut directory_file).await;

        let bucket_count = load_bucket_file(&mut buckets_file).await;

        Self {
            directory_file,
            bucket_count,
            buckets_file,
            bucket_lookup: bucket_addresses,
            global_level,
        }
    }

    pub async fn handle_cmd(&mut self, cmd: Command) -> Result<(), ()> {
        match cmd {
            Command::Put(cmd) => self
                .put(Record(hash_string_key(&cmd.0), cmd.1.into_bytes()))
                .await
                .unwrap(),
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

    fn hash_key_to_remainder(&self, key: &str) -> (u64, usize) {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        let hash = hasher.finish();
        let remainder = hash % 2_u64.pow(self.global_level.into());
        (hash, remainder.try_into().unwrap())
    }

    fn hash_to_remainder(&self, hash: u64) -> usize {
        (hash % 2_u64.pow(self.global_level.into()))
            .try_into()
            .unwrap()
    }

    async fn put(&mut self, record: Record) -> Result<(), ()> {
        // Look up the address of the bucket
        let remainder = self.hash_to_remainder(record.0);
        let bucket_index = self.bucket_lookup[remainder as usize];

        // Load the bucket
        let mut bucket = Bucket::read_from_file(&mut self.buckets_file, bucket_index).await;

        // Put command in or split the bucket

        loop {
            // Easy case: It fits;
            if bucket.remaining_byte_space >= record.byte_len() {
                bucket.records.push(record);
                bucket.update_remaining_byte_count();
                bucket.save_to_file(&mut self.buckets_file).await;
                return Ok(());
            }

            // Bucket split
            bucket.level += 1;
            let og_remainder = remainder as u64 % 2_u64.pow(bucket.level.into());

            // Split the bucket in half into 2 Vec<Records> one with the new bucket and one with the original bucket
            let (original, new) = bucket.records.drain(..bucket.records.len()).fold(
                (vec![], vec![]),
                |(mut og_, mut new_), x| {
                    let remainder = x.0 % 2_u64.pow(bucket.level.into());
                    if remainder > og_remainder {
                        new_.push(x)
                    } else {
                        og_.push(x)
                    }
                    (og_, new_)
                },
            );

            // Original bucket
            bucket.records = original;

            // New bucket
            let mut new_bucket = Bucket {
                level: bucket.level,
                records: new,
                bucket_index: self.bucket_count as usize,
                remaining_byte_space: 0,
            };

            // Save both buckets
            bucket.update_remaining_byte_count();
            new_bucket.update_remaining_byte_count();
            bucket.save_to_file(&mut self.buckets_file).await;
            new_bucket.save_to_file(&mut self.buckets_file).await;

            // Local split
            if bucket.level <= self.global_level {
                // Grab all the lookup entries which point to the existing bucket
                let mut indices = vec![];
                for i in 0..self.bucket_lookup.len() {
                    if self.bucket_lookup[i] as usize == bucket.bucket_index {
                        indices.push(i);
                    }
                }

                // Re-adjust the lookup
                for index in &indices {
                    let remainder = index % 2_usize.pow(bucket.level.into());
                    if remainder as u64 > og_remainder {
                        self.bucket_lookup[*index] = self.bucket_count;
                    }
                }
            } else {
                // Global split

                // Readjust the indices
                self.bucket_lookup.extend(self.bucket_lookup.clone());
                self.bucket_lookup[(self.bucket_count as u64 - 1 + og_remainder) as usize] =
                    self.bucket_count;

                self.global_level += 1;
            }

            // Re-assign bucket to the new bucket which the record matches against hash of the
            // record
            let new_remainder = record.0 % 2_u64.pow(bucket.level.into());

            if new_remainder > og_remainder {
                bucket = new_bucket;
            }
        }
    }

    pub async fn get(&mut self, cmd: &GetCommand) -> Result<Option<String>, ()> {
        let (hash, remainder) = self.hash_key_to_remainder(&cmd.0);

        let bucket =
            Bucket::read_from_file(&mut self.buckets_file, self.bucket_lookup[remainder]).await;

        Ok(bucket
            .records
            .into_iter()
            .find(|x| x.0 == hash)
            .map(|r| String::from_utf8(r.1).unwrap()))
    }

    pub async fn delete(&mut self, cmd: DeleteCommand) -> Result<(), ()> {
        let (hash, remainder) = self.hash_key_to_remainder(&cmd.0);

        let mut bucket =
            Bucket::read_from_file(&mut self.buckets_file, self.bucket_lookup[remainder]).await;

        bucket.records = bucket.records.into_iter().filter(|x| x.0 != hash).collect();
        bucket.update_remaining_byte_count();
        bucket.save_to_file(&mut self.buckets_file).await;
        Ok(())
    }
}

/// Rust representation of a bucket
///
/// ## Binary layout
///
/// - First byte is the level
/// - Rest are records
///
pub struct Bucket {
    /// The nth bucket in the bucket file, 0 indexed
    bucket_index: usize,

    /// The local level of the bucket
    level: u8,

    /// The number of bytes remaining available in the bucket
    remaining_byte_space: usize,

    /// The records contained in the bucket
    records: Vec<Record>,
}

impl Bucket {
    fn parse_from_bytes(bytes: &[u8], bucket_number: usize) -> Result<(Self, &[u8]), ()> {
        let mut page = &bytes[0..PAGE_SIZE];
        let level = bytes[0];

        let mut records = vec![];
        loop {
            if page.len() == 0 {
                break;
            }
            if page[0] == 0 {
                page = &page[1..]
            } else if let Ok((record, rest_page)) = Record::parse_from_bytes(page) {
                records.push(record);
                page = rest_page;
            }
        }

        let mut bucket = Bucket {
            bucket_index: bucket_number,
            level,
            records,
            remaining_byte_space: 0,
        };

        bucket.update_remaining_byte_count();

        Ok((bucket, &bytes[PAGE_SIZE..]))
    }

    fn update_remaining_byte_count(&mut self) {
        let records_byte_len: usize = self.records.iter().map(|r| r.byte_len()).sum();
        self.remaining_byte_space = PAGE_SIZE - BUCKET_HEADER - records_byte_len
    }

    async fn read_from_file(file: &mut File, bucket_index: u32) -> Self {
        file.seek(SeekFrom::Start(
            (1 + (bucket_index as usize) * PAGE_SIZE) as u64,
        ))
        .await
        .unwrap();
        let mut buf = [0; PAGE_SIZE];
        file.read_exact(&mut buf).await.unwrap();
        let (bucket, _) = Self::parse_from_bytes(&buf, bucket_index as usize).unwrap();
        bucket
    }

    async fn save_to_file(&self, file: &mut File) {
        file.seek(SeekFrom::Start((1 + self.bucket_index * PAGE_SIZE) as u64))
            .await
            .unwrap();
        let mut buf = [0_u8; PAGE_SIZE];
        buf[0] = self.level;
        let mut ptr = 1_usize;
        // TODO: Decide what to do with this clone
        for record in self.records.clone().into_iter() {
            let bytes = record.into_bytes();
            let length = bytes.len();
            for i in ptr..ptr + length {
                buf[i] = bytes[i];
            }
            ptr = ptr + length;
        }
    }
}

/// The record stored in the database
///
/// Made of the hash the u64, and the binary data
///
/// ## Binary representation
///
/// - 1 byte header starting with 1 to indicate this is not empty space
/// - The next 8 bytes are the hash
/// - The next 2 bytes is the length of the value
/// - Followed by the value in bytes
#[derive(Clone)]
struct Record(u64, Vec<u8>);

impl Record {
    fn into_bytes(self) -> Vec<u8> {
        let mut result = vec![];
        result.push(1);
        result.extend(self.0.to_le_bytes());
        result.extend((self.1.len() as u16).to_le_bytes());
        result.extend(self.1);
        return result;
    }

    fn byte_len(&self) -> usize {
        HASH_RECORD_HEADER + HASH_LENGTH + HASH_VALUE_HEADER + self.1.len()
    }

    fn parse_from_bytes(bytes: &[u8]) -> Result<(Self, &[u8]), ()> {
        if bytes[0] != 1 {
            return Err(());
        }
        let hash = u64::from_le_bytes(bytes[1..9].try_into().unwrap());
        let len = u16::from_le_bytes(bytes[9..11].try_into().unwrap());
        let value = bytes[11..(11 + len) as usize].to_owned();
        Ok((Record(hash, value), &bytes[(11 + len as usize)..]))
    }
}

#[cfg(test)]
mod test {
    use super::*;

    async fn get_engine(test_prefix: String) -> HashStorage {
        let test_data_prefx = String::from("./test_data");
        let mut data_file = test_data_prefx.clone();
        data_file.push_str(&test_prefix);
        data_file.push_str("_data.db");
        let mut dir_file = test_data_prefx.clone();
        dir_file.push_str(&test_prefix);
        dir_file.push_str("_dir.db");
        HashStorage::new(&dir_file, &data_file).await
    }
}
