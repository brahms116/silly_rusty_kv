use silly_rusty_kv::*;
use std::io::{stdin, IsTerminal};

#[tokio::main]
async fn main() {
    // Determine if we are running in a terminal
    let is_terminal = stdin().is_terminal();

    if is_terminal {
        run_repl().await;
    } else {
        process_script_from_stdin().await;
    }
}
