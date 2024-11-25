#![allow(unused_imports)]

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

async fn handle_client(mut stream: TcpStream) {
    let mut buf = [0; 512];
    loop {
        let read_count = stream.read(&mut buf).await.unwrap();
        if read_count == 0 {
            break;
        }
        stream.write_all(b"+PONG\r\n").await.unwrap();
    }
    stream.flush().await.unwrap();
}

#[tokio::main]
async fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    // Uncomment this block to pass the first stage
    //
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
    loop {
        let stream = listener.accept().await;
        match stream {
            Ok((stream, _)) => {
                tokio::spawn(async move {
                    handle_client(stream).await;
                });
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
