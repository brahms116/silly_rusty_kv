use super::storage::*;
use crate::{execute::execute_command, setup::setup_db};
use tokio::io::{stdin, AsyncBufReadExt, BufReader};
use tokio::select;
use tokio::sync::oneshot::{channel, Receiver};

use super::command::*;

pub async fn run_repl() {
    let (mut storage, index) = setup_db();

    // Make a oneshot
    // Send the sender to another task awaiting ctrl-c
    // Send the receiver to inner loop who does a select! between waiting
    // for input and waiting for ctrl-c

    let (sender, mut receiver) = channel::<()>();

    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.unwrap();
        sender.send(()).unwrap();
    });

    println!("Welcome to Silly Rusty KV!");
    loop {
        if inner_loop(&mut storage, index, &mut receiver).await {
            break;
        };
    }
    println!("Goodbye!");
    std::process::exit(0);
}

async fn execute_user_input(storage: &mut StorageEngine, index: (), input: Option<String>) -> bool {
    if let None = input {
        return true;
    }

    let cmd = input.unwrap().parse::<Command>();

    if let Err(err) = cmd {
        println!("Error: {}", err);
    } else if let Ok(cmd) = cmd {
        let should_quit = cmd == Command::Exit;
        // execute command
        let out_stream = ();
        execute_command(cmd.clone(), index, storage, out_stream).await;
        return should_quit;
    }
    return false;
}

async fn inner_loop(storage: &mut StorageEngine, index: (), receiver: &mut Receiver<()>) -> bool {
    let mut reader = BufReader::new(stdin()).lines();

    select! {
        _ = receiver => {
            println!("Received ctrl-c");
            return execute_user_input(storage, index, Some("EXIT".into())).await;
        }
        input = reader.next_line() => {
            return execute_user_input(storage, index, input.unwrap()).await;
        }
    }
}
