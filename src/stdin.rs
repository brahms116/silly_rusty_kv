use crate::command::*;
use crate::execute::*;
use crate::setup::*;

use tokio::select;
use tokio::{
    io::{stdin, AsyncBufReadExt, AsyncRead, BufReader},
    sync::{mpsc, oneshot},
};

pub async fn process_from_stdin() {
    let (send, mut recv) = mpsc::channel::<String>(100);
    let (ctlrs, mut ctlrc) = oneshot::channel::<()>();

    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.unwrap();
        ctlrs.send(()).unwrap();
    });

    let read_task = tokio::spawn(async move {
        read_line_from_stdin(BufReader::new(stdin()), &send).await;
    });

    let process_task = tokio::spawn(async move {
        process_lines_from_stdin(&mut recv, &mut ctlrc).await;
    });

    tokio::try_join!(read_task, process_task).unwrap();
}

async fn process_lines_from_stdin(
    reciever: &mut mpsc::Receiver<String>,
    ctlrc_signal: &mut oneshot::Receiver<()>,
) {
    let (mut storage, mut wal) = setup_db().await;
    let mut transaction_id = None;
    while let Some(line) = reciever.recv().await {
        select! {
            _ = &mut *ctlrc_signal => {
                break;
            }
            output = execute_user_input(&mut storage, &mut wal, &line, transaction_id.as_deref()) => {
                match output {
                    Ok(output) => {
                        handle_command_output_for_transaction_id(&output, &mut transaction_id);
                        println!("{}", output);
                    },
                    Err(e) => {println!("{}", e)}
                }
            }
        }
    }
    execute_command(&mut storage, &mut wal, UserCommand::Exit, None)
        .await
        .unwrap();
}

async fn read_line_from_stdin<R: AsyncRead + Unpin>(
    reader: BufReader<R>,
    send: &mpsc::Sender<String>,
) {
    let mut lines = reader.lines();
    while let Some(line) = lines.next_line().await.unwrap() {
        send.send(line).await.unwrap();
    }
}
