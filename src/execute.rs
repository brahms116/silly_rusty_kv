use crate::hash_storage::HashStorage;

use super::command::*;

pub async fn execute_user_input(
    storage: &mut HashStorage,
    input: &str,
) -> Result<CommandOutput, String> {
    let cmd = input.parse::<StorageCommand>()?;
    // execute command
    execute_command(storage, cmd.clone())
        .await
        .map_err(|_| "Error executing command".to_string())
}

pub async fn execute_command(storage: &mut HashStorage, cmd: StorageCommand) -> Result<CommandOutput, ()> {
    storage.handle_cmd(cmd).await
}
