use super::storage::*;
use crate::{execute::execute_command, setup::setup_db};

use super::command::*;

pub async fn run_repl() {
    let (mut storage, index) = setup_db();
    println!("Welcome to Silly Rusty KV!");
    loop {
        if inner_loop(&mut storage, index).await {
            break;
        };
    }
    println!("Goodbye!");
}

async fn inner_loop(storage: &mut StorageEngine, index: ()) -> bool {
    let mut input = String::new();
    // read line from stdin
    std::io::stdin().read_line(&mut input).unwrap();

    // parse command
    let cmd = input.parse::<Command>();

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
