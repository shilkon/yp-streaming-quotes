
use std::fs::File;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{Shutdown, TcpStream, UdpSocket};
use std::sync::Arc;
use std::thread;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use clap::Parser;

use yp_streaming_quotes::StockQuote;

#[derive(Parser)]
struct CliArgs {
    server: String,
    client: String,
    tickers: String
}

fn main() -> anyhow::Result<()> {
    let args = CliArgs::parse();
    let tickers_file = File::open(args.tickers)?;
    let tickers: Vec<String> = BufReader::new(tickers_file)
        .lines()
        .map(|line| line.unwrap().trim().to_string())
        .collect();

    let mut server_stream = TcpStream::connect(&args.server)?;

    let cmd = format!("STREAM {} {}", args.client, tickers.join(","));
    server_stream.write(&cmd.as_bytes())?;
    server_stream.shutdown(Shutdown::Write)?;

    let server_socket = UdpSocket::bind(args.client)?;
    server_socket.connect(args.server)?;

    let server_socket_ping = server_socket.try_clone()?;
    let is_active = Arc::new(AtomicBool::new(true));
    let is_active_ping = Arc::clone(&is_active);
    thread::spawn(move || {
        while is_active_ping.load(Ordering::Relaxed) {
            if let Err(e) = server_socket_ping.send("Ping".as_bytes()) {
                eprintln!("Unable to send PING to server");
            }
            thread::sleep(Duration::from_secs(2)); //make random delay
        }
    });

    let mut buf = [0u8; 1024];
    while is_active.load(Ordering::Relaxed) {
        match server_socket.recv(&mut buf) {
            Ok(amt) => {
                let decoded: Vec<StockQuote> = postcard::from_bytes(&buf[..amt])?;
                for stock_quote in &decoded {
                    println!("{}", stock_quote.ticker)
                }
            },
            Err(_) => todo!(),
        }
    }

    Ok(())
}
