use crate::command::*;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash as _, Hasher};
use std::io::SeekFrom;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

// ### CONSTANTS
// Define some constants so that we don't have to do manual arithmetic when dealing with ranges in
// byte slices

/// Number of bytes in a page
const PAGE_BYTES: usize = 4096;

/// The type used to represent the local bucket level in the hash table
type BucketLevel = u8;

/// The length in bytes of `BucketLevel`
const BUCKET_HEADER_BYTES: usize = (BucketLevel::BITS / 8) as usize;

/// The header to indicate that a record is present and not empty space, see `Record` for the full
/// layout
const RECORD_HEADER: u8 = 1;

/// The length of the record header in bytes
const RECORD_HEADER_BYTES: usize = 1;

/// Type representing a hash in the hash table
type Hash = u64;

/// The length in bytes of a hash
const HASH_BYTES: usize = (Hash::BITS / 8) as usize;

/// The type used to indicate the len of the value component in a record, see `Record` for the full layout
type RecordValueLength = u16;

/// The len in bytes for the value header in a record, see `Record` for the full layout
const RECORD_VALUE_HEADER_BYTES: usize = (RecordValueLength::BITS / 8) as usize;

/// The maximum length in bytes in which a value in a record can be
const MAX_HASH_VALUE_BYTES: usize =
    PAGE_BYTES - BUCKET_HEADER_BYTES - RECORD_HEADER_BYTES - HASH_BYTES - RECORD_VALUE_HEADER_BYTES;

/// Type used to represent the total bucket count in the hash table
type BucketCount = u32;

/// The length in bytes of `BucketCount`
const BUCKET_COUNT_BYTES: usize = (BucketCount::BITS / 8) as usize;

/// The type used to reference and index buckets in the hash table
type BucketIndex = BucketCount;

/// The length in bytes of `BucketIndex`
const BUCKET_INDEX_BYTES: usize = (BucketIndex::BITS / 8) as usize;

/// The type used to define the global level of the hash table
type GlobalLevel = u8;

/// The length in bytes of `GlobalLevel`
const GLOBAL_LEVEL_BYTES: usize = (GlobalLevel::BITS / 8) as usize;

// ### Utils

/// Returns a hash from a string key
fn hash_string_key(key: &str) -> Hash {
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    hasher.finish()
}

/// Given an length of the directory addresses derive the
/// global level
///
/// This should just be log base 2 of the global level or
/// the position of most significant bit
fn addr_count_to_global_level(mut length: usize) -> GlobalLevel {
    let mut result: GlobalLevel = 0;
    while length > 1 {
        length = length >> 1;
        result += 1;
    }
    return result;
}

/// Reads the bucket count from the "buckets file" of the hash table, see the `buckets_file` field of `HashStorage`
/// for more details
async fn load_buckets_file(buckets_file: &mut File) -> BucketCount {
    if buckets_file.metadata().await.unwrap().len() == 0 {
        // setup the file by pushing an empty bucket to it
        let bucket = Bucket {
            records: vec![],
            level: 0,
            bucket_index: 0,
            // Doesn't matter
            remaining_byte_space: 0,
        };
        bucket.save_to_file(buckets_file).await;
        return 1;
    }
    buckets_file.seek(SeekFrom::Start(0)).await.unwrap();
    let mut buf = [0; BUCKET_COUNT_BYTES];
    buckets_file.read_exact(&mut buf).await.unwrap();
    return BucketCount::from_le_bytes(buf);
}

/// Saves the bucket count into the "buckets file" of the hash table, see the `buckets_file` field of `HashStorage`
/// for more details
async fn save_buckets_file(bucket_count: BucketCount, buckets_file: &mut File) {
    buckets_file.seek(SeekFrom::Start(0)).await.unwrap();
    let buf = bucket_count.to_le_bytes();
    return buckets_file.write_all(&buf).await.unwrap();
}

/// Loads the directory file of the hash table
///
/// # Returns
/// - A tuple containing the directory and the global level of the hash table
async fn load_directory(directory_file: &mut File) -> (Vec<BucketIndex>, GlobalLevel) {
    // Return if the file is empty
    if directory_file.metadata().await.unwrap().len() == 0 {
        return (vec![0], 0);
    }

    directory_file.seek(SeekFrom::Start(0)).await.unwrap();

    let mut global_level_buf = [0; GLOBAL_LEVEL_BYTES];
    directory_file
        .read_exact(&mut global_level_buf)
        .await
        .unwrap();
    let global_level = GlobalLevel::from_le_bytes(global_level_buf);

    let addr_count = 2_usize.pow(global_level.into());

    let mut buf = vec![0; addr_count * BUCKET_INDEX_BYTES];
    directory_file.read_exact(&mut buf).await.unwrap();

    let mut result = vec![0; addr_count];
    for i in 0..addr_count {
        let start = BUCKET_INDEX_BYTES * i;
        result[i] =
            BucketIndex::from_le_bytes(buf[start..start + BUCKET_INDEX_BYTES].try_into().unwrap());
    }
    return (result, global_level);
}

/// Saves the directory of the hash table into the directory file
async fn save_directory(vec: &Vec<BucketIndex>, directory_file: &mut File) {
    let addr_count = vec.len();
    let global_level = addr_count_to_global_level(addr_count);

    directory_file.seek(SeekFrom::Start(0)).await.unwrap();
    let global_level_buf = global_level.to_le_bytes();
    directory_file.write_all(&global_level_buf).await.unwrap();

    let mut buf = vec![0; vec.len() * BUCKET_INDEX_BYTES];
    for i in 0..addr_count {
        let bytes = vec[i].to_le_bytes();
        let start = BUCKET_INDEX_BYTES * i;
        for k in 0..BUCKET_INDEX_BYTES {
            buf[start + k] = bytes[k]
        }
    }
    directory_file.write_all(&buf).await.unwrap();
}

/// Hash storage engine utilising extensible hashing
pub struct HashStorage {
    /// The file containing the look up table and the global level
    ///
    /// # File layout
    /// - First `GLOBAL_LEVEL_BYTES` is the global level
    /// - Next is followed by the list of `BucketIndex`s stored in LE
    /// The length of this list is 2^global_level
    ///
    /// There are no pages in this file, the entire file is loaded and saved all at once
    directory_file: File,

    /// The file containing the buckets
    ///
    /// # File layout
    /// - First `BUCKET_HEADER_BYTES` is the number of current buckets
    /// - Followed by pages of PAGE_SIZE, with each page being a bucket
    buckets_file: File,

    /// The current number of buckets, we need this to know
    /// where to create new buckets, loaded from the buckets file
    bucket_count: BucketCount,

    /// The global level of the index
    ///
    /// Saved and loaded from the directory file
    global_level: GlobalLevel,

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
    bucket_lookup: Vec<BucketIndex>,
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

        let bucket_count = load_buckets_file(&mut buckets_file).await;

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
            Command::Exit => self.exit().await,
        }
        Ok(None)
    }

    async fn exit(&mut self) {
        save_directory(&self.bucket_lookup, &mut self.directory_file).await;
        save_buckets_file(self.bucket_count, &mut self.buckets_file).await;
        self.directory_file.sync_all().await.unwrap();
        self.buckets_file.sync_all().await.unwrap();
    }

    fn hash_key_to_remainder(&self, key: &str) -> (Hash, usize) {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        let hash = hasher.finish();
        let remainder = hash % 2_u64.pow(self.global_level.into());
        (hash, remainder.try_into().unwrap())
    }

    fn hash_to_remainder(&self, hash: Hash) -> usize {
        (hash % 2_u64.pow(self.global_level.into()))
            .try_into()
            .unwrap()
    }

    fn debug(&self) {
        println!(
            "Bucket count: {}, Bucket_lookup: {:?}, Global lvl: {}",
            self.bucket_count, self.bucket_lookup, self.global_level
        );
    }

    async fn put(&mut self, record: Record) -> Result<(), ()> {
        // Look up the address of the bucket
        let bucket_index = self.bucket_lookup[self.hash_to_remainder(record.0)];

        // Load the bucket
        let mut bucket = Bucket::read_from_file(&mut self.buckets_file, bucket_index).await;

        // Put command in or split the bucket

        // self.debug();

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
                self.hash_to_remainder(record.0) % 2_usize.pow(bucket.level.into());

            bucket.level += 1;

            // Split the bucket in half into 2 Vec<Records> one with the new bucket and one with the original bucket
            let (original, new) = bucket.records.drain(..bucket.records.len()).fold(
                (vec![], vec![]),
                |(mut og_, mut new_), x| {
                    let remainder = x.0 % 2_u64.pow(bucket.level.into());
                    if remainder > og_bucket_remainder as u64 {
                        new_.push(x)
                    } else {
                        og_.push(x)
                    }
                    (og_, new_)
                },
            );

            // Original bucket
            bucket.records = original;

            let new_bucket_index = self.bucket_count;

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
                    if self.bucket_lookup[i] == bucket.bucket_index {
                        indices.push(i);
                    }
                }

                // Re-adjust the lookup
                for index in &indices {
                    let remainder = index % 2_usize.pow(bucket.level.into());
                    if remainder > og_bucket_remainder {
                        self.bucket_lookup[*index] = new_bucket_index as u32;
                    }
                }
            } else {
                // Global split

                // Readjust the indices
                let old_len = self.bucket_lookup.len();
                self.bucket_lookup.extend(self.bucket_lookup.clone());
                self.bucket_lookup[old_len + og_bucket_remainder] = new_bucket_index as u32;

                self.global_level += 1;
            }

            // Re-assign "bucket" to the new bucket which the record matches against hash of the
            // record. This is because it might need to split again
            let new_remainder = record.0 % 2_u64.pow(bucket.level.into());
            if new_remainder > og_bucket_remainder as u64 {
                bucket = new_bucket;
            }
        }
    }

    async fn get(&mut self, hash: Hash) -> Result<Option<Vec<u8>>, ()> {
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
/// - First `BUCKET_HEADER_BYTES` indicate the local level of the bucket
/// - Rest are records
///
#[derive(PartialEq, Debug, Clone)]
pub struct Bucket {
    /// The nth bucket in the bucket file, 0 indexed
    bucket_index: BucketIndex,

    /// The local level of the bucket
    level: BucketLevel,

    /// The number of bytes remaining available in the bucket
    remaining_byte_space: usize,

    /// The records contained in the bucket
    records: Vec<Record>,
}

impl Bucket {
    fn parse_from_bytes(bytes: &[u8], bucket_index: BucketIndex) -> Result<(Self, &[u8]), ()> {
        let mut page = &bytes[0..PAGE_BYTES];
        let level = page[0];
        page = &page[1..];

        let mut records = vec![];
        loop {
            if page.len() == 0 {
                break;
            }
            if page[0] == 0 {
                page = &page[1..]
            } else {
                let (record, rest_page) = Record::parse_from_bytes(page).unwrap();
                records.push(record);
                page = rest_page;
            }
        }

        let mut bucket = Bucket {
            bucket_index,
            level,
            records,
            remaining_byte_space: 0,
        };

        bucket.update_remaining_byte_count();

        Ok((bucket, &bytes[PAGE_BYTES..]))
    }

    fn update_remaining_byte_count(&mut self) {
        let records_byte_len: usize = self.records.iter().map(|r| r.byte_len()).sum();
        self.remaining_byte_space = PAGE_BYTES - BUCKET_HEADER_BYTES - records_byte_len
    }

    async fn read_from_file(file: &mut File, bucket_index: BucketIndex) -> Self {
        file.seek(SeekFrom::Start(
            (BUCKET_COUNT_BYTES + (bucket_index as usize) * PAGE_BYTES) as u64,
        ))
        .await
        .unwrap();
        let mut buf = [0; PAGE_BYTES];
        file.read_exact(&mut buf).await.unwrap();
        let (bucket, _) = Self::parse_from_bytes(&buf, bucket_index).unwrap();
        bucket
    }

    async fn save_to_file(&self, file: &mut File) {
        file.seek(SeekFrom::Start(
            (BUCKET_COUNT_BYTES + self.bucket_index as usize * PAGE_BYTES) as u64,
        ))
        .await
        .unwrap();
        let mut buf = [0_u8; PAGE_BYTES];
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

#[cfg(test)]
mod test_bucket {
    use super::*;
    use crate::test::*;

    #[tokio::test]
    async fn to_and_from_file() {
        let mut bucket = Bucket {
            bucket_index: 0,
            level: 1,
            remaining_byte_space: 0,
            records: vec![
                Record(0b_1110, vec![25, 236, 36, 46]),
                Record(0b_0010, vec![26, 236, 36, 46]),
                Record(0b_0110, vec![27, 236, 36, 46]),
            ],
        };
        bucket.update_remaining_byte_count();

        let mut file = reset_or_create_file("./test_data/test_bucket_to_and_from_file").into();
        bucket.save_to_file(&mut file).await;

        let bucket_ = Bucket::read_from_file(&mut file, 0).await;
        assert_eq!(bucket_, bucket);
    }
}

/// The record stored in the database
///
/// Made of the hash of type `Hash` and the associated binary data
///
/// ## Binary representation
///
/// - A record header of `RECORD_HEADER_BYTES` in length
///     - This has a value of 1, indicating that it is not empty space
/// - The hash containing `HASH_BYTES` in length
/// - Record value header indicating the length of the value, has a length of
/// `RECORD_VALUE_HEADER_BYTES`
/// - The bytes containing the value with the length indicated by the record's value header
///
#[derive(Clone, Debug, PartialEq)]
struct Record(Hash, Vec<u8>);

impl Record {
    fn into_bytes(self) -> Vec<u8> {
        let mut result = vec![];
        result.push(RECORD_HEADER);
        result.extend(self.0.to_le_bytes());
        result.extend((self.1.len() as RecordValueLength).to_le_bytes());
        result.extend(self.1);
        return result;
    }

    fn byte_len(&self) -> usize {
        RECORD_HEADER_BYTES + HASH_BYTES + RECORD_VALUE_HEADER_BYTES + self.1.len()
    }

    fn parse_from_bytes(bytes: &[u8]) -> Result<(Self, &[u8]), ()> {
        if bytes[0] != 1 {
            return Err(());
        }

        // Some indicies
        let record_hash_start = RECORD_HEADER_BYTES;
        let record_value_header_start = record_hash_start + HASH_BYTES;
        let record_value_start = record_value_header_start + RECORD_VALUE_HEADER_BYTES;

        let hash = Hash::from_le_bytes(
            bytes[record_hash_start..record_value_header_start]
                .try_into()
                .unwrap(),
        );
        let len = RecordValueLength::from_le_bytes(
            bytes[record_value_header_start..record_value_start]
                .try_into()
                .unwrap(),
        );
        let value = bytes[record_value_start..(record_value_start + len as usize)].to_owned();
        Ok((
            Record(hash, value),
            &bytes[(record_value_start + len as usize)..],
        ))
    }
}

#[cfg(test)]
mod test_record {
    use super::*;

    #[test]
    fn into_and_from_bytes() {
        let r = Record(0b_1110, vec![25, 236, 36, 46]);
        let bytes = r.clone().into_bytes();
        let (r_, bs) = Record::parse_from_bytes(&bytes).unwrap();
        assert_eq!(r_, r);
        assert_eq!(bs.len(), 0);
    }
}

#[cfg(test)]
mod test_hash_storage {
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

    async fn get_engine_without_reset(test_prefix: &str) -> HashStorage {
        let test_data_prefx = String::from("./test_data");
        let data_path = format!("{}/{}_data.db", test_data_prefx, test_prefix);
        let dir_path = format!("{}/{}_dir.db", test_data_prefx, test_prefix);
        HashStorage::new(&dir_path, &data_path).await
    }

    fn record_from_size(hash: u64, byte: u8, size: usize) -> Record {
        let value_len = size - RECORD_HEADER_BYTES - HASH_BYTES - RECORD_VALUE_HEADER_BYTES;
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

    #[tokio::test]
    async fn exit_save_load() {
        let mut engine = get_engine("hash_storage_exit_save_load").await;

        engine.bucket_lookup = vec![1, 5, 6, 7, 2, 4, 7, 8];
        engine.global_level = 3;
        engine.bucket_count = 8;

        engine.handle_cmd(Command::Exit).await.unwrap();

        let engine_reloaded = get_engine_without_reset("hash_storage_exit_save_load").await;

        assert_eq!(engine_reloaded.bucket_lookup, engine.bucket_lookup);
        assert_eq!(engine_reloaded.global_level, engine.global_level);
        assert_eq!(engine_reloaded.bucket_count, engine.bucket_count);
    }
}
