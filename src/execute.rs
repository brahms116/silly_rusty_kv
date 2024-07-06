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
    execute_command(storage, wal, cmd, transaction_id).await
}

pub async fn execute_command(
    storage: &mut HashStorage,
    wal: &mut Wal,
    cmd: UserCommand,
    transaction_id: Option<&str>,
) -> Result<CommandOutput, String> {
    if let Some(id) = transaction_id {
        match cmd {
            UserCommand::Get(ref cmd) => {
                let output = wal.get(id, &cmd);
                if let Some(output) = output {
                    if let Some(value) = output {
                        return Ok(CommandOutput::Found(value));
                    }
                    return Ok(CommandOutput::NotFound(cmd.0.clone()));
                }
            }
            UserCommand::Put(cmd) => {
                wal.mutate(id, Mutation::Put(cmd)).unwrap();
                return Ok(CommandOutput::Put);
            }
            UserCommand::Delete(cmd) => {
                wal.mutate(id, Mutation::Delete(cmd)).unwrap();
                return Ok(CommandOutput::Delete);
            }
            _ => {}
        };
    }

    match cmd {
        UserCommand::Get(cmd) => storage.handle_cmd(StorageCommand::Get(cmd)).await,
        UserCommand::Put(cmd) => storage.handle_cmd(StorageCommand::Put(cmd)).await,
        UserCommand::Delete(cmd) => storage.handle_cmd(StorageCommand::Delete(cmd)).await,
        UserCommand::Exit => storage.handle_cmd(StorageCommand::Flush).await,
        UserCommand::Begin => Ok(CommandOutput::Begin(wal.begin())),
        UserCommand::Commit => {
            let muts = wal
                .retrieve_mutations(transaction_id.unwrap_or(""))
                .ok_or("Not in a transaction")?;
            for m in muts {
                match m {
                    Mutation::Put(c) => storage.handle_cmd(StorageCommand::Put(c)).await?,
                    Mutation::Delete(c) => storage.handle_cmd(StorageCommand::Delete(c)).await?,
                };
            }
            Ok(CommandOutput::Commit)
        }
        UserCommand::Rollback => {
            wal.retrieve_mutations(transaction_id.unwrap_or(""))
                .ok_or("Not in a transaction")?;
            Ok(CommandOutput::Rollback)
        }
    }
}
