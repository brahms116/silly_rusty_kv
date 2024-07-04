use silly_rusty_kv::*;

enum Mode {
    Repl,
    Stdin,
    Server,
}

#[tokio::main]
async fn main() {
    let mut mode = Mode::Server;

    let args: Vec<String> = std::env::args().collect();
    if let Some(arg) = args.get(1) {
        match arg as &str {
            "--repl" => mode = Mode::Repl,
            "--stdin" => mode = Mode::Stdin,
            _ => {}
        }
    }

    match mode {
        Mode::Repl => run_repl().await,
        Mode::Stdin => process_from_stdin().await,
        Mode::Server => run_server().await,
    }
}
