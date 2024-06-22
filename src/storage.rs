use std::ops::RangeBounds;

use crate::command::*;
use crate::consts::*;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

pub struct StorageEngine {
    file: File,
    read_file: File,
    pub wal_buffer: Vec<Mutation>,
    remaining_space_for_wal: usize,
    unused_page_size: usize,
    num_pages: usize,
}

impl StorageEngine {
    pub fn new(file_path: &str) -> Self {
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(file_path)
            .unwrap();

        let read_file = std::fs::OpenOptions::new()
            .read(true)
            .open(file_path)
            .unwrap();

        // Get the size of the file
        let file_size = read_file.metadata().unwrap().len() as usize;

        // Get the size of the last page
        let last_page_size = file_size % PAGE_SIZE;

        let num_pages = file_size / PAGE_SIZE;

        // Get the remaining space for the wal
        let remaining_space_for_wal = PAGE_SIZE - last_page_size;

        // println!(
        //     "File size: {}, last page size: {}, num pages: {}, remaining space for wal: {}",
        //     file_size, last_page_size, num_pages, remaining_space_for_wal
        // );
        Self {
            file: file.into(),
            read_file: read_file.into(),
            wal_buffer: Vec::new(),
            remaining_space_for_wal,
            num_pages,
            unused_page_size: last_page_size,
        }
    }

    pub async fn handle_cmd(&mut self, cmd: Command, out_stream: ()) -> Result<(), ()> {
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
            Command::Exit => {
                self.flush_wal(false).await.unwrap();
            }
        }
        Ok(())
    }

    pub async fn put(&mut self, cmd: PutCommand) -> Result<(), ()> {
        // Check if the wal buffer has space for the mutation
        if cmd.byte_len() > self.remaining_space_for_wal {
            self.flush_wal(true).await.unwrap();
        }
        self.remaining_space_for_wal -= cmd.byte_len();
        self.wal_buffer.push(Mutation::Put(cmd));
        Ok(())
    }

    pub async fn get_from_wal(&mut self, cmd: &GetCommand) -> Result<Option<Option<&str>>, ()> {
        let wal_len = self.wal_buffer.len();
        for i in 0..wal_len {
            if let Mutation::Delete(delete_cmd) = &self.wal_buffer[wal_len - i - 1] {
                if delete_cmd.0 == cmd.0 {
                    return Ok(Some(None));
                }
            }
            if let Mutation::Put(put_cmd) = &self.wal_buffer[wal_len - i - 1] {
                if put_cmd.0 == cmd.0 {
                    return Ok(Some(Some(&put_cmd.1)));
                }
            }
        }
        Ok(None)
    }

    /// Temp hack with no indices
    pub async fn get(&mut self, cmd: &GetCommand) -> Result<Option<String>, ()> {
        // self.debug(..).await;
        {
            let wal_value = self.get_from_wal(cmd).await.unwrap();
            if let Some(value) = wal_value {
                return Ok(value.map(|value| value.to_string()));
            }
        }

        let current_page_pointer = self.num_pages * PAGE_SIZE;
        if self.unused_page_size > 0 {
            let mut buf = vec![0; self.unused_page_size];
            self.read_file
                .seek(std::io::SeekFrom::Start(current_page_pointer as u64))
                .await
                .unwrap();
            self.read_file.read_exact(&mut buf).await.unwrap();

            if let Some(value) = get_value_from_buffer(buf.into_iter(), &cmd.0).unwrap() {
                return Ok(value);
            }
        }

        // Check the previous pages
        for i in 0..self.num_pages {
            let mut buf = vec![0; PAGE_SIZE];
            self.read_file
                .seek(std::io::SeekFrom::Start(
                    (self.num_pages - 1 - i) as u64 * PAGE_SIZE as u64,
                ))
                .await
                .unwrap();
            self.read_file.read_exact(&mut buf).await.unwrap();

            if let Some(value) = get_value_from_buffer(buf.into_iter(), &cmd.0).unwrap() {
                return Ok(value);
            }
        }
        Ok(None)
    }

    pub async fn delete(&mut self, cmd: DeleteCommand) -> Result<(), ()> {
        // Check if the wal buffer has space for the mutation
        if cmd.byte_len() > self.remaining_space_for_wal {
            self.flush_wal(true).await.unwrap();
        }
        self.remaining_space_for_wal -= cmd.byte_len();
        self.wal_buffer.push(Mutation::Delete(cmd));
        Ok(())
    }

    /// Debug function to parse and print the contents of the file page by page
    ///
    /// # Arguments
    /// * `pages` - The range of pages to print
    pub async fn debug(&mut self, pages: impl RangeBounds<usize>) {
        // Get the starting page
        let start_page = match pages.start_bound() {
            std::ops::Bound::Included(start_page) => *start_page,
            std::ops::Bound::Excluded(start_page) => *start_page + 1,
            std::ops::Bound::Unbounded => 0,
        };

        // Get the ending page
        let end_page = match pages.end_bound() {
            std::ops::Bound::Included(end_page) => *end_page,
            std::ops::Bound::Excluded(end_page) => *end_page - 1,
            std::ops::Bound::Unbounded => self.num_pages,
        };

        // If the end page exceeds the number of pages, set it to the number of pages
        let end_page = if end_page > self.num_pages {
            self.num_pages
        } else {
            end_page
        };

        // Loop through the pages
        for page in start_page..end_page {
            let mut buf = vec![0; PAGE_SIZE];
            self.read_file
                .seek(std::io::SeekFrom::Start(page as u64 * PAGE_SIZE as u64))
                .await
                .unwrap();
            self.read_file.read_exact(&mut buf).await.unwrap();

            // Print the page number with a start marker
            println!("Page: {} START", page);

            // Collect the mutations from the buffer as a vector or mutations
            let mutations = parse_buffer_to_mutations(buf.into_iter()).unwrap();

            // Print the mutations
            for mutation in mutations {
                println!("{:?}", mutation);
            }

            // Print the page number with an end marker
            println!("Page: {} END", page);
        }
    }

    pub async fn flush_wal(&mut self, fill_remaining_space: bool) -> Result<(), ()> {
        // println!("Flushing wal");
        // Try to not reallocate?
        let bytes = self
            .wal_buffer
            .drain(..)
            .map(|mutation| {
                // println!("mutation: {:?}", mutation);
                mutation.into_bytes()
            })
            .flatten()
            .collect::<Vec<u8>>();

        self.file.write_all(&bytes).await.unwrap();

        if fill_remaining_space {
            self.file
                .write_all(&vec![0; self.remaining_space_for_wal])
                .await
                .unwrap();
        }

        self.file.sync_all().await.unwrap();
        self.remaining_space_for_wal = PAGE_SIZE;
        self.unused_page_size = 0;
        self.num_pages += 1;
        Ok(())
    }

    pub async fn get_value(&mut self, addr: usize) -> Result<&str, ()> {
        todo!()
    }
}
