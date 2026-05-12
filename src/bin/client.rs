
use std::fs::File;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{Shutdown, TcpStream, UdpSocket};
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
    let mut buf = [0u8; 1024];
    loop {
        match server_socket.recv(&mut buf) {
            Ok(_) => {
                let decoded: Vec<StockQuote> = postcard::from_bytes(&buf)?;
                for stock_quote in &decoded {
                    println!("{}", stock_quote.ticker)
                }
            },
            Err(_) => todo!(),
        }
    }

    Ok(())
}
