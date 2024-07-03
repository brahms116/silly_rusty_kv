use crate::hash_storage::HashStorage;
use crate::{execute::*, setup::setup_db};
use tokio::io::{stdin, AsyncBufReadExt, BufReader};
use tokio::select;
use tokio::sync::oneshot::{channel, Receiver};

use super::command::*;

pub async fn run_repl() {
    let mut storage = setup_db().await;

    let (sender, mut receiver) = channel::<()>();

    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.unwrap();
        sender.send(()).unwrap();
    });

    println!("Welcome to Silly Rusty KV!");
    loop {
        if inner_loop(&mut storage, &mut receiver).await {
            break;
        };
    }
    println!("Goodbye!");
    std::process::exit(0);
}

async fn inner_loop(storage: &mut HashStorage, receiver: &mut Receiver<()>) -> bool {
    let mut reader = BufReader::new(stdin()).lines();

    select! {
        _ = receiver => {
            // TODO: Handle reciever error
            println!("Received ctrl-c");
            let output = execute_command(storage, Command::Exit).await.unwrap();
            match output {
                CommandOutput::Exit => return true,
                _ => return false
            }
        }
        input = reader.next_line() => {
            if let Some(input) = input.unwrap() {
                let output = execute_user_input(storage, &input).await.unwrap();
                println!("{}", output);
                match output {
                    CommandOutput::Exit => return true,
                    _ => return false
                }
            }
            return true
        }
    }
}
