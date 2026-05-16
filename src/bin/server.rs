use std::collections::HashSet;
use std::fs::File;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::Arc;
use std::{io, thread};
use std::io::{BufRead, BufReader, Read};

use yp_streaming_quotes::{StockQuote, QuotesServer};

static TCP_ADDRESS: &'static str = "127.0.0.1:7878";
static TICKERS_FILE: &'static str = "tickers.txt";

fn main() -> anyhow::Result<()> {
    env_logger::init();

    let server = Arc::new(QuotesServer::new(TCP_ADDRESS)?);
    
    // TODO: use CTRL + C to safely exit app and threads

    let tickers_file = File::open(TICKERS_FILE)?;
    let quotes: Vec<StockQuote> = BufReader::new(tickers_file)
        .lines()
        .map(|line| StockQuote::new(
            line.expect("Failed to read ticker").trim())
        )
        .collect();
    let generator_server = server.clone();
    thread::spawn(move || {
        generator_server.generate_qoutes(quotes);
    });

    let server_ping = Arc::clone(&server);
    thread::spawn(move || {
        server_ping.ping();
    });

    let server_keep_alive = Arc::clone(&server);
    thread::spawn(move || {
        server_keep_alive.keep_alive();
    });

    let listener: TcpListener = TcpListener::bind(TCP_ADDRESS)?;
    log::info!("Server listening on port 7878");

    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                let client_server = Arc::clone(&server);
                if let Err(e) = handle_client(&mut stream, client_server) {
                    log::error!("Error while processing client command: {e}");
                }
            }
            Err(e) => log::error!("Connection failed: {}", e),
        }
    }

    log::info!("Server shutdown");

    Ok(())
}

fn handle_client(stream: &mut TcpStream, server: Arc<QuotesServer>) -> io::Result<()> {
    let mut cmd = String::new();
    match stream.read_to_string(&mut cmd) {
        Ok(n) if n > 0 => {
            let mut parts = cmd.split_whitespace();
            match parts.next() {
                Some("STREAM") => {
                    let address = parts.next().map(|s| s.parse::<SocketAddr>().unwrap());
                    let tickets = parts.next().map(|s| {
                        s.split(',').map(String::from).collect::<HashSet<String>>()
                    });
                    if let (Some(address), Some(tickets)) = (address, tickets) {
                        if tickets.len() > 0 {
                            let thread_server = server.clone();
                            thread::spawn(move || {
                                thread_server.stream_quotes(address, tickets)
                            });
                        }
                    }
                }
                Some(k) => log::error!("Unsupported command keyword '{k}'"),
                None => log::error!("Expected command keyword")
            }
        }
        Ok(_) => log::info!("Recieved empty command from '{}'", stream.peer_addr()?),
        Err(e) => log::error!("Recieved command from '{}' with error: {}", stream.peer_addr()?, e)
    }
    Ok(())
}
