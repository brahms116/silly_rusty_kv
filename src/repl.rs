use super::storage::*;
use crate::hash_storage::HashStorage;
use crate::{execute::execute_command, setup::setup_db};
use tokio::io::{stdin, AsyncBufReadExt, BufReader};
use tokio::select;
use tokio::sync::oneshot::{channel, Receiver};

use super::command::*;

pub async fn run_repl() {
    let (mut storage, mut hash_storage) = setup_db().await;

    let (sender, mut receiver) = channel::<()>();

    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.unwrap();
        sender.send(()).unwrap();
    });

    println!("Welcome to Silly Rusty KV!");
    loop {
        if inner_loop(&mut storage, &mut hash_storage, &mut receiver).await {
            break;
        };
    }
    println!("Goodbye!");
    std::process::exit(0);
}

pub async fn execute_user_input(
    storage: &mut StorageEngine,
    hash_storage: &mut HashStorage,
    input: Option<String>,
) -> bool {
    if let None = input {
        return true;
    }

    let cmd = input.unwrap().parse::<Command>();

    if let Err(err) = cmd {
        println!("Error: {}", err);
    } else if let Ok(cmd) = cmd {
        let should_quit = cmd == Command::Exit;
        // execute command
        execute_command(cmd.clone(), storage, hash_storage).await;
        return should_quit;
    }
    return false;
}

async fn inner_loop(
    storage: &mut StorageEngine,
    hash_storage: &mut HashStorage,
    receiver: &mut Receiver<()>,
) -> bool {
    let mut reader = BufReader::new(stdin()).lines();

    select! {
        _ = receiver => {
            // TODO: Handle reciever error
            println!("Received ctrl-c");
            return execute_user_input(storage, hash_storage, Some("EXIT".into())).await;
        }
        input = reader.next_line() => {
            return execute_user_input(storage, hash_storage, input.unwrap()).await;
        }
    }
}
