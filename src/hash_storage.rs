use crate::bytes::*;
use crate::consts::PAGE_SIZE;
use std::io::SeekFrom;
/// Extensible hashing storage
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

use crate::command::*;

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

async fn load_bucket_file(file: &mut File) -> u64 {
    if file.metadata().await.unwrap().len() == 0 {
        return 0;
    }
    file.seek(SeekFrom::Start(0)).await.unwrap();
    return file.read_u64_le().await.unwrap();
}

async fn save_bucket_file(bucket_count: u64, file: &mut File) {
    file.seek(SeekFrom::Start(0)).await.unwrap();
    let buf = bucket_count.to_le_bytes();
    return file.write_all(&buf).await.unwrap();
}

async fn load_directory(file: &mut File) -> (Vec<u64>, u8) {
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
        result.push(u64::from_le_bytes([
            buf[start],
            buf[start + 1],
            buf[start + 2],
            buf[start + 3],
            buf[start + 4],
            buf[start + 5],
            buf[start + 6],
            buf[start + 7],
        ]));
    }
    return (result, global_level);
}

async fn save_directory(vec: &Vec<u64>, file: &mut File) {
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
        buf[start + 4] = bytes[4];
        buf[start + 5] = bytes[5];
        buf[start + 6] = bytes[6];
        buf[start + 7] = bytes[7];
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
    /// - Next is followed by the list of u64s stored in LE
    /// The length of this list is 2^global_level
    ///
    /// There are no pages in this file, the entire file is loaded and saved all at once
    directory_file: File,

    /// The file containing the buckets
    ///
    /// ## File layout
    /// - First u64 is the number of current buckets
    /// - Followed by pages of PAGE_SIZE, with each page being a bucket
    buckets_file: File,

    /// The current number of buckets, we need this to know
    /// where to create new buckets, loaded from the buckets file
    bucket_count: u64,

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
            bucket_addresses,
            global_level,
        }
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
        // 1. Hash the key
        // 2. Conside the last n bits, with N being the global level
        // 3. Look up the address of the bucket
        // 4. Load the bucket
        // 5. Put command in or split the bucket

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
    /// The nth bucket in the bucket file
    bucket_number: u64,

    /// The local level of the bucket
    level: u8,

    /// The number of bytes remaining available in the bucket
    remaining_byte_space: usize,

    /// The records contained in the bucket
    records: Vec<PutCommand>,
}

impl IntoBytes for Bucket {
    fn into_bytes(self) -> Vec<u8> {
         
    }
}


impl<T> ParseFromBytes<T> for Bucket
where
    T: Iterator<Item = u8>,
{
    type Error = ();

    fn from_bytes(mut bytes: T) -> Result<(Self, T), Self::Error> {
        // Take page size from it because we need to know the remaining byte size
        let mut page = bytes.by_ref().take(PAGE_SIZE).peekable();
        let mut records: Vec<PutCommand> = vec![];

        loop {
            if page.next_if(|x| *x == 0).is_some() {
                break;
            }
            let (cmd, rest) = PutCommand::from_bytes(page).map_err(|_| ())?;
            records.push(cmd);
            page = rest;
        }

        let remaining_byte_space = page.count();

        Ok((
            Bucket {
                bucket_number: 0,
                level: 0,
                records,
                remaining_byte_space,
            },
            bytes,
        ))
    }
}
