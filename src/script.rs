use crate::repl::*;
use crate::setup::*;

use tokio::{
    io::{stdin, AsyncBufReadExt, AsyncRead, BufReader},
    sync::{mpsc, oneshot},
};

pub async fn process_script_from_stdin() {
    let (send, mut recv) = mpsc::channel::<String>(100);
    let (ctlrs, mut ctlrc) = oneshot::channel::<()>();

    let read_task = tokio::spawn(async move {
        read_line_from_script(BufReader::new(stdin()), &send).await;
    });

    let process_task = tokio::spawn(async move {
        process_lines_from_script(&mut recv, &mut ctlrc).await;
    });

    tokio::try_join!(read_task, process_task).unwrap();
}

async fn process_lines_from_script(
    reciever: &mut mpsc::Receiver<String>,
    ctlrc_signal: &mut oneshot::Receiver<()>,
) {
    // TODO: Need to handle the ctlrc_signal

    let (mut storage, index) = setup_db();
    while let Some(line) = reciever.recv().await {
        execute_user_input(&mut storage, index, Some(line)).await;
    }
    execute_user_input(&mut storage, index, Some("EXIT".to_string())).await;
}

async fn read_line_from_script<R: AsyncRead + Unpin>(
    reader: BufReader<R>,
    send: &mpsc::Sender<String>,
) {
    let mut lines = reader.lines();
    while let Some(line) = lines.next_line().await.unwrap() {
        send.send(line).await.unwrap();
    }
}
