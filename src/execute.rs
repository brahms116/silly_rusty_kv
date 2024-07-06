use crate::hash_storage::HashStorage;

use super::command::*;

pub async fn execute_user_input(
    storage: &mut HashStorage,
    input: &str,
) -> Result<CommandOutput, String> {
    let cmd = input.parse::<UserCommand>()?;
    // execute command
    execute_command(storage, cmd)
        .await
        .map_err(|_| "Error executing command".to_string())
}

pub async fn execute_command(storage: &mut HashStorage, cmd: UserCommand) -> Result<CommandOutput, ()> {
    let cmd = match cmd {
        UserCommand::Get(cmd) => StorageCommand::Get(cmd),
        UserCommand::Put(cmd) => StorageCommand::Put(cmd),
        UserCommand::Delete(cmd) => StorageCommand::Delete(cmd),
        UserCommand::Exit => StorageCommand::Flush,
        _ => return Err(()),
    };
    storage.handle_cmd(cmd).await
}

pub async fn execute_storage_command(
    storage: &mut HashStorage,
    cmd: StorageCommand,
) -> Result<CommandOutput, ()> {
    storage.handle_cmd(cmd).await
}
