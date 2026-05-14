use std::collections::HashSet;
use std::fs::File;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::Arc;
use std::thread;
use std::io::{BufRead, BufReader, Read};

use yp_streaming_quotes::{StockQuote, QuotesServer};

fn main() -> std::io::Result<()> {
    let server_address = "127.0.0.1:7878";
    let listener: TcpListener = TcpListener::bind(server_address)?;
    println!("Server listening on port 7878");

    let server = Arc::new(QuotesServer::new(server_address)?);
    
    // TODO: use CTRL + C to safely exit app and threads

    let tickers_file = File::open("tickers.txt")?;
    let quotes: Vec<StockQuote> = BufReader::new(tickers_file)
            .lines()
            .map(|line| StockQuote::new(line.unwrap().trim()))
            .collect();
    let generator_server = server.clone();
    let generator_handle = thread::spawn(move || {
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

    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                let mut cmd = String::new();
                match stream.read_to_string(&mut cmd) {
                    Ok(n) if n > 0 => {
                        let mut parts = cmd.split_whitespace();
                        if let Some("STREAM") = parts.next() {
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
                    }
                    Ok(_) => continue,
                    Err(e) => continue
                }
                // stream.shutdown(Shutdown::Read)?; 
            }
            Err(e) => eprintln!("Connection failed: {}", e),
        }
    }

    Ok(())
}
