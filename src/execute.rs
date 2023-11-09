use crate::storage::StorageEngine;

use super::command::*;

pub async fn execute_command(cmd: Command, index: (), storage:&mut StorageEngine, out_stream: ()) {
    storage.handle_cmd(cmd, out_stream).await.unwrap();
}

