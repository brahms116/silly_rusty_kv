use crate::command::*;
use crate::consts::*;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

pub struct StorageEngine {
    file: File,
    // Tmp hack
    read_file: File,
    wal_buffer: Vec<Mutation>,
    remaining_space_for_wal: usize,
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

        // Get the remaining space for the wal
        let remaining_space_for_wal = PAGE_SIZE - last_page_size;

        Self {
            file: file.into(),
            read_file: read_file.into(),
            wal_buffer: Vec::new(),
            remaining_space_for_wal,
        }
    }

    pub async fn handle_cmd(&mut self, cmd: Command, out_stream: ()) -> Result<(), ()> {
        todo!()
    }

    pub async fn put(&mut self, cmd: PutCommand) -> Result<(), ()> {
        // Check if the wal buffer has space for the mutation
        if cmd.byte_len() > self.remaining_space_for_wal {
            self.flush_wal().await.unwrap();
        }
        self.wal_buffer.push(Mutation::Put(cmd));
        Ok(())
    }

    pub async fn get_from_wal(&mut self, cmd: &GetCommand) -> Result<Option<&str>, ()> {
        let wal_len = self.wal_buffer.len();
        for i in 0..wal_len {
            if let Mutation::Put(put_cmd) = &self.wal_buffer[wal_len - i - 1] {
                if put_cmd.0 == cmd.0 {
                    return Ok(Some(&put_cmd.1));
                }
            }
        }
        Ok(None)
    }

    /// Temp hack with no indices
    pub async fn get(&mut self, cmd: &GetCommand) -> Result<Option<String>, ()> {
        {
            let wal_value = self.get_from_wal(cmd).await.unwrap();
            if let Some(value) = wal_value {
                return Ok(Some(value.into()));
            }
        }
        // read the whole file into memory
        let mut buf = Vec::new();
        self.read_file.read_to_end(&mut buf).await.unwrap();

        Ok(None)
    }

    pub async fn delete(&mut self, cmd: DeleteCommand) -> Result<(), ()> {
        // Check if the wal buffer has space for the mutation
        if cmd.byte_len() > self.remaining_space_for_wal {
            self.flush_wal().await.unwrap();
        }
        self.wal_buffer.push(Mutation::Delete(cmd));
        Ok(())
    }

    pub async fn flush_wal(&mut self) -> Result<(), ()> {
        // Try to not reallocate?
        self.file
            .write_all(
                &self
                    .wal_buffer
                    .drain(..)
                    .map(|mutation| mutation.into_bytes())
                    .flatten()
                    .collect::<Vec<u8>>(),
            )
            .await
            .unwrap();

        self.remaining_space_for_wal = PAGE_SIZE;
        Ok(())
    }

    pub async fn get_value(&mut self, addr: usize) -> Result<&str, ()> {
        todo!()
    }
}
