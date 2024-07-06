use crate::command::*;
use crate::execute::*;

use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
    select,
    signal::{
        ctrl_c,
        unix::{signal, SignalKind},
    },
    spawn,
    sync::mpsc::{channel, Receiver, Sender},
    sync::oneshot::{channel as one_channel, Receiver as OneReceiver, Sender as OneSender},
};

use crate::setup::setup_db;

pub async fn run_server() {
    let listener = TcpListener::bind("127.0.0.1:5476").await.unwrap();
    let (s, mut r) = channel::<SendLine>(100);

    let (ctlrs, mut ctlrc) = one_channel::<()>();
    let (breaklooprs, mut breaklooprc) = one_channel::<()>();

    spawn(async move {
        let mut sigterm = signal(SignalKind::terminate()).unwrap();
        select! {
            _ = sigterm.recv() => {},
            _ = ctrl_c() => {}
        }

        ctlrs.send(()).unwrap();
        breaklooprs.send(()).unwrap();
    });

    let engine_task = spawn(async move { receive_line_single_thread(&mut r, &mut ctlrc).await });

    loop {
        select! {
            _ = &mut breaklooprc => {
                break;
            },
            res = listener.accept() => {
                let (soc, _) = res.unwrap();
                let s = s.clone();
                spawn(async move {
                    handle_socket(soc, &s).await;
                });
            }
        }
    }

    engine_task.await.unwrap()
}

async fn handle_socket(s: TcpStream, send: &Sender<SendLine>) {
    let (r, mut w) = s.into_split();
    let mut lines = BufReader::new(r).lines();
    while let Some(line) = lines.next_line().await.unwrap() {
        println!("Received: {}", line);
        let (s, res) = one_channel::<String>();
        send.send(SendLine { line, cb: s }).await.unwrap();
        let output = format!("{}\n", res.await.unwrap());
        w.write_all(&output.into_bytes()).await.unwrap();
    }
}

pub struct SendLine {
    line: String,
    cb: OneSender<String>,
}

async fn receive_line_single_thread(r: &mut Receiver<SendLine>, ctrlc: &mut OneReceiver<()>) {
    let mut storage = setup_db().await;
    loop {
        select! {
            _ = &mut *ctrlc => {
                break;
            }
            msg = r.recv() => {
                if let Some(SendLine {line, cb}) = msg {
                   let output = execute_user_input(&mut storage, &line).await.map(|x| x.to_string()).unwrap_or_else(|e| e);
                   cb.send(output).unwrap();
                } else {
                    break;
                }
            }
        }
    }
    execute_storage_command(&mut storage, StorageCommand::Flush)
        .await
        .unwrap();
}
