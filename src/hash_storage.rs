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

    pub async fn handle_cmd(&mut self, cmd: Command) -> Result<Option<String>, ()> {
        match cmd {
            Command::Put(cmd) => self
                .put(Record(hash_string_key(&cmd.0), cmd.1.into_bytes()))
                .await
                .unwrap(),
            Command::Delete(cmd) => self.delete(cmd).await.unwrap(),
            Command::Get(cmd) => {
                if let Some(value) = self
                    .get(hash_string_key(&cmd.0))
                    .await
                    .unwrap()
                    .map(|x| String::from_utf8(x).unwrap())
                {
                    println!("{}", value);
                    return Ok(Some(value));
                } else {
                    println!("Key not found");
                }
            }
            Command::Exit => {}
        }
        Ok(None)
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
        let bucket_index = self.bucket_lookup[self.hash_to_remainder(record.0)];

        // Load the bucket
        let mut bucket = Bucket::read_from_file(&mut self.buckets_file, bucket_index).await;

        // Put command in or split the bucket

        loop {
            // Easy cases, they fit
            if let Some(existing_record) = bucket.records.iter_mut().find(|x| x.0 == record.0) {
                // Check the difference between the existing record and the new record to see if
                // the bucket can accomodate to the difference in size
                if existing_record.1.len() >= record.1.len()
                    || record.1.len() - existing_record.1.len() <= bucket.remaining_byte_space
                {
                    existing_record.1 = record.1;
                    bucket.update_remaining_byte_count();
                    bucket.save_to_file(&mut self.buckets_file).await;
                    return Ok(());
                }
            } else if bucket.remaining_byte_space >= record.byte_len() {
                bucket.records.push(record);
                bucket.update_remaining_byte_count();
                bucket.save_to_file(&mut self.buckets_file).await;
                return Ok(());
            }

            // Bucket split

            // This is the original remainder for the bucket before the split, we have to call
            // self.hash_to_remainder again as the global level may have changed between the
            // iterations of the loop due to a global split
            let og_bucket_remainder =
                self.hash_to_remainder(record.0) as u64 % 2_u64.pow(bucket.level.into());

            bucket.level += 1;

            // Split the bucket in half into 2 Vec<Records> one with the new bucket and one with the original bucket
            let (original, new) = bucket.records.drain(..bucket.records.len()).fold(
                (vec![], vec![]),
                |(mut og_, mut new_), x| {
                    let remainder = x.0 % 2_u64.pow(bucket.level.into());
                    if remainder > og_bucket_remainder {
                        new_.push(x)
                    } else {
                        og_.push(x)
                    }
                    (og_, new_)
                },
            );

            // Original bucket
            bucket.records = original;

            let new_bucket_index = self.bucket_count as usize;

            // New bucket
            let mut new_bucket = Bucket {
                level: bucket.level,
                records: new,
                bucket_index: new_bucket_index,
                remaining_byte_space: 0,
            };

            // Save both buckets
            bucket.update_remaining_byte_count();
            new_bucket.update_remaining_byte_count();
            bucket.save_to_file(&mut self.buckets_file).await;
            new_bucket.save_to_file(&mut self.buckets_file).await;

            self.bucket_count += 1;

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
                    if remainder as u64 > og_bucket_remainder {
                        self.bucket_lookup[*index] = new_bucket_index as u32;
                    }
                }
            } else {
                // Global split

                // Readjust the indices
                let old_len = self.bucket_lookup.len();
                self.bucket_lookup.extend(self.bucket_lookup.clone());
                self.bucket_lookup[old_len + og_bucket_remainder as usize] =
                    new_bucket_index as u32;

                self.global_level += 1;
            }

            // Re-assign "bucket" to the new bucket which the record matches against hash of the
            // record. This is because it might need to split again
            let new_remainder = record.0 % 2_u64.pow(bucket.level.into());
            if new_remainder > og_bucket_remainder {
                bucket = new_bucket;
            }
        }
    }

    async fn get(&mut self, hash: u64) -> Result<Option<Vec<u8>>, ()> {
        let remainder = self.hash_to_remainder(hash);
        let bucket =
            Bucket::read_from_file(&mut self.buckets_file, self.bucket_lookup[remainder]).await;

        Ok(bucket
            .records
            .into_iter()
            .find(|x| x.0 == hash)
            .map(|r| r.1))
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
        let level = page[0];
        page = &page[1..];

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
                buf[i] = bytes[i - ptr];
            }
            ptr = ptr + length;
        }

        file.write_all(&buf).await.unwrap();
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
#[derive(Clone, Debug)]
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
    use crate::test::*;

    async fn get_engine(test_prefix: &str) -> HashStorage {
        let test_data_prefx = String::from("./test_data");
        let data_path = format!("{}/{}_data.db", test_data_prefx, test_prefix);
        let dir_path = format!("{}/{}_dir.db", test_data_prefx, test_prefix);
        reset_or_create_file(&data_path);
        reset_or_create_file(&dir_path);
        HashStorage::new(&dir_path, &data_path).await
    }

    fn record_from_size(hash: u64, byte: u8, size: usize) -> Record {
        let value_len = size - HASH_RECORD_HEADER - HASH_LENGTH - HASH_VALUE_HEADER;
        let value = vec![byte; value_len];
        Record(hash, value)
    }

    #[tokio::test]
    async fn smoke() {
        let mut engine = get_engine("hash_storage_smoke").await;
        let cmd = PutCommand("MY_KEY".into(), "MY_VALUE".into());
        let get_cmd = GetCommand("MY_KEY".into());

        engine.handle_cmd(cmd.clone().into()).await.unwrap();
        let retrieved = engine
            .handle_cmd(get_cmd.clone().into())
            .await
            .unwrap()
            .unwrap();

        assert_eq!(retrieved, "MY_VALUE");

        let cmd = PutCommand("MY_KEY".into(), "MY_VALUE2".into());
        engine.handle_cmd(cmd.clone().into()).await.unwrap();
        let retrieved = engine
            .handle_cmd(get_cmd.clone().into())
            .await
            .unwrap()
            .unwrap();

        assert_eq!(retrieved, "MY_VALUE2");

        engine
            .handle_cmd(DeleteCommand("MY_KEY".into()).into())
            .await
            .unwrap();

        let retrieved = engine.handle_cmd(get_cmd.clone().into()).await.unwrap();
        assert_eq!(retrieved, None);
    }

    /// Simulate a scenario as the following:
    ///
    /// Bucket index 0 has a a record of a hash ending with 1010 with 4000 bytes in value.
    /// Bucket index 0 has is at a local level of 1.
    /// The global level is at 3.
    /// We are going to insert a record with a hash of 1110 with 4000 bytes of value, forcing
    /// it to recursively local split twice.
    ///
    /// 000 -> 0 -> Current record
    /// 001 -> 1
    /// 010 -> 0
    /// 011 -> 2
    /// 100 -> 0
    /// 101 -> 3
    /// 110 -> 0
    /// 111 -> 4
    ///
    ///
    /// We should expect the following after the insert
    ///
    /// 000 -> 0
    /// 001 -> 1
    /// 010 -> 5 -> Old record
    /// 011 -> 2
    /// 100 -> 0
    /// 101 -> 3
    /// 110 -> 6 -> New record
    /// 111 -> 4
    ///
    #[tokio::test]
    async fn local_split() {
        let mut engine = get_engine("hash_storage_local_split").await;
        let old_record = record_from_size(0b_1010, 1, 4000);

        let new_record = record_from_size(0b_1110, 2, 4000);

        let mut buckets = vec![
            Bucket {
                bucket_index: 0,
                remaining_byte_space: 0,
                level: 1,
                records: vec![old_record],
            },
            Bucket {
                level: 3,
                remaining_byte_space: 0,
                records: vec![],
                bucket_index: 1,
            },
            Bucket {
                level: 3,
                remaining_byte_space: 0,
                records: vec![],
                bucket_index: 2,
            },
            Bucket {
                level: 3,
                remaining_byte_space: 0,
                records: vec![],
                bucket_index: 3,
            },
            Bucket {
                level: 3,
                remaining_byte_space: 0,
                records: vec![],
                bucket_index: 4,
            },
        ];
        for bucket in &mut buckets {
            bucket.update_remaining_byte_count();
            bucket.save_to_file(&mut engine.buckets_file).await;
        }

        engine.bucket_count = 5;
        engine.global_level = 3;

        engine.bucket_lookup = vec![0, 1, 0, 2, 0, 3, 0, 4];

        engine.put(new_record).await.unwrap();

        assert_eq!(engine.global_level, 3);
        assert_eq!(engine.bucket_count, 7);
        assert_eq!(engine.bucket_lookup, vec![0, 1, 5, 2, 0, 3, 6, 4]);

        let old_record_bucket = Bucket::read_from_file(&mut engine.buckets_file, 5).await;
        old_record_bucket
            .records
            .iter()
            .find(|x| x.0 == 0b_1010)
            .unwrap();
        assert_eq!(old_record_bucket.level, 3);

        let new_record_bucket = Bucket::read_from_file(&mut engine.buckets_file, 6).await;
        new_record_bucket
            .records
            .iter()
            .find(|x| x.0 == 0b_1110)
            .unwrap();
        assert_eq!(new_record_bucket.level, 3);
    }

    /// Simulate a similar scenario as the local split
    ///
    /// We will use 2 records, both 4000 bytes in length. Same as the local split test
    /// we will have record 1 with a hash of 1010 and the second with 1110.
    ///
    /// But instead of having the global level at 3, we have a global level of 1. So inserting both
    /// record number 2 should cause a global split twice.
    ///
    /// 0 -> 0 -> Current record
    /// 1 -> 1 -> Current record
    ///
    ///
    /// We should expect the following after the insert
    /// 00 -> 0
    /// 01 -> 1
    /// 10 -> 2
    /// 11 -> 1
    ///
    /// 000 -> 0
    /// 001 -> 1
    /// 010 -> 2 -> Old Record
    /// 011 -> 1
    /// 100 -> 0
    /// 101 -> 1
    /// 110 -> 3 -> New Record
    /// 111 -> 1
    ///
    #[tokio::test]
    async fn global_split() {
        let mut engine = get_engine("hash_storage_global_split").await;
        let old_record = record_from_size(0b_1010, 1, 4000);

        let new_record = record_from_size(0b_1110, 2, 4000);

        let mut buckets = vec![
            Bucket {
                bucket_index: 0,
                remaining_byte_space: 0,
                level: 1,
                records: vec![old_record],
            },
            Bucket {
                level: 1,
                remaining_byte_space: 0,
                records: vec![],
                bucket_index: 1,
            },
        ];
        for bucket in &mut buckets {
            bucket.update_remaining_byte_count();
            bucket.save_to_file(&mut engine.buckets_file).await;
        }
        engine.bucket_count = 2;
        engine.global_level = 1;
        engine.bucket_lookup = vec![0, 1];

        engine.put(new_record).await.unwrap();

        assert_eq!(engine.global_level, 3);
        assert_eq!(engine.bucket_count, 4);
        assert_eq!(engine.bucket_lookup, vec![0, 1, 2, 1, 0, 1, 3, 1]);

        let old_record_bucket = Bucket::read_from_file(&mut engine.buckets_file, 2).await;
        old_record_bucket
            .records
            .iter()
            .find(|x| x.0 == 0b_1010)
            .unwrap();
        assert_eq!(old_record_bucket.level, 3);

        let new_record_bucket = Bucket::read_from_file(&mut engine.buckets_file, 3).await;
        new_record_bucket
            .records
            .iter()
            .find(|x| x.0 == 0b_1110)
            .unwrap();
        assert_eq!(new_record_bucket.level, 3);
    }
}
