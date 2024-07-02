use silly_rusty_kv::*;

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let is_repl = if let Some(arg) = args.get(1) {
        arg == "--repl"
    } else {
        false
    };

    if is_repl {
        run_repl().await;
    } else {
        process_from_stdin().await;
    }
}
