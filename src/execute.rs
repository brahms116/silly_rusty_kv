use crate::{hash_storage::HashStorage, storage::StorageEngine};

use super::command::*;

pub async fn execute_command(
    cmd: Command,
    storage: &mut StorageEngine,
    hash_storage: &mut HashStorage,
) {
    // storage.handle_cmd(cmd, out_stream).await.unwrap();
    hash_storage.handle_cmd(cmd).await.unwrap();
}
