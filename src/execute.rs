use crate::hash_storage::HashStorage;

use super::command::*;

pub async fn execute_command(
    cmd: Command,
    storage: &mut HashStorage,
) {
    // storage.handle_cmd(cmd, out_stream).await.unwrap();
    storage.handle_cmd(cmd).await.unwrap();
}
