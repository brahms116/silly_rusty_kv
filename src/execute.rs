use crate::{hash_storage::HashStorage, wal::Wal};

use super::command::*;

pub async fn execute_user_input(
    storage: &mut HashStorage,
    wal: &mut Wal,
    input: &str,
    transaction_id: Option<&str>,
) -> Result<CommandOutput, String> {
    let cmd = input.parse::<UserCommand>()?;
    // execute command
    execute_command(storage, wal, cmd, transaction_id)
        .await
        .map_err(|_| "Error executing command".to_string())
}

pub async fn execute_command(
    storage: &mut HashStorage,
    wal: &mut Wal,
    cmd: UserCommand,
    transaction_id: Option<&str>,
) -> Result<CommandOutput, ()> {
    let cmd = match cmd {
        UserCommand::Get(cmd) => StorageCommand::Get(cmd),
        UserCommand::Put(cmd) => StorageCommand::Put(cmd),
        UserCommand::Delete(cmd) => StorageCommand::Delete(cmd),
        UserCommand::Exit => StorageCommand::Flush,
        _ => return Err(()),
    };
    storage.handle_cmd(cmd).await
}
