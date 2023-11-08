use crate::{setup::setup_db, execute::execute_command};

use super::command::*;

pub async fn run_repl() {

    println!("Welcome to Silly Rusty KV!");
    loop {
        if inner_loop().await {
            break
        }; 
    }
    println!("Goodbye!");
}

async fn inner_loop() -> bool {
    let mut input = String::new();
    // read line from stdin
    std::io::stdin().read_line(&mut input).unwrap();

    if input.trim() == "EXIT" {
        return true
    }

    let (mut storage, index) = setup_db();

    // parse command
    let cmd = input.parse::<Command>();

    if let Err(err) = cmd {
        println!("Error: {}", err);
    } else if let Ok(cmd) = cmd {
        // execute command
        let out_stream = ();
        execute_command(cmd, index, &mut storage, out_stream).await;
    }
    return false
}
