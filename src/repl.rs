use crate::command::*;
use crate::hash_storage::HashStorage;
use crate::wal::Wal;
use crate::{execute::*, setup::setup_db};
use tokio::io::{stdin, AsyncBufReadExt, BufReader};
use tokio::select;
use tokio::sync::oneshot::{channel, Receiver};

pub async fn run_repl() {
    let (mut storage, mut wal) = setup_db().await;

    let (sender, mut receiver) = channel::<()>();

    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.unwrap();
        sender.send(()).unwrap();
    });

    println!("Welcome to Silly Rusty KV!");
    let mut transaction_id = None;
    loop {
        if inner_loop(&mut storage, &mut wal, &mut receiver, &mut transaction_id).await {
            break;
        };
    }
    println!("Goodbye!");
    std::process::exit(0);
}

async fn inner_loop(
    storage: &mut HashStorage,
    wal: &mut Wal,
    receiver: &mut Receiver<()>,
    transaction_id: &mut Option<String>,
) -> bool {
    let mut reader = BufReader::new(stdin()).lines();

    select! {
        _ = receiver => {
            // TODO: Handle reciever error
            println!("Received ctrl-c");
            let output = execute_command(storage, wal,UserCommand::Exit, None).await.unwrap();
            match output {
                CommandOutput::Exit => return true,
                _ => return false
            }
        }
        input = reader.next_line() => {
            if let Some(input) = input.unwrap() {
                let output = execute_user_input(storage, wal, &input, transaction_id.as_deref()).await;
                if let Ok(output) = output {
                    println!("{}", output);
                    handle_command_output_for_transaction_id(&output, transaction_id);
                    match output {
                        CommandOutput::Exit => return true,
                        _ => return false
                    }
                } else {
                    let output = output.unwrap_err();
                    println!("{}", output);
                }
            }
            return true
        }
    }
}
