use crate::command::{Command, DeleteCommand, GetCommand, PutCommand};
use tokio::fs::File;

pub struct StorageEngine {
    file: File,
    wal_buffer: Vec<Command>,
    remaining_space_for_wal: usize,
}

impl StorageEngine {
    pub fn new(file_path: &str) -> Self {
        todo!()
    }

    pub fn handle_cmd(&mut self, cmd: Command, out_stream: ()) -> Result<(), ()> {
        todo!()
    }

    pub async fn put(&mut self, cmd: &PutCommand) -> Result<(), ()> {
        todo!()
    }

    /// Temp hack with no indices
    pub async fn get(&mut self, cmd: &GetCommand) -> Result<&str, ()> {
        todo!()
    }

    pub async fn delete(&mut self, cmd: &DeleteCommand) -> Result<(), ()> {
        todo!()
    }

    pub async fn flush_wal(&mut self) -> Result<(), ()> {
        todo!()
    }

    pub async fn get_value(&mut self, addr: usize) -> Result<&str, ()> {
        todo!()
    }
}
