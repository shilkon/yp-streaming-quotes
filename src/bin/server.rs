use std::collections::HashSet;
use std::fs::File;
use std::net::{Shutdown, TcpListener, TcpStream};
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

    // TODO: start UDP listener thread for PING

    let tickers_file = File::open("tickers.txt")?;
    let generator_server = server.clone();
    let generator_handle = thread::spawn(move || {
        let mut quotes: Vec<StockQuote> = BufReader::new(tickers_file)
            .lines()
            .map(|line| StockQuote::new(line.unwrap().trim()))
            .collect();
        while generator_server.is_active() {
            quotes.iter_mut().for_each(|q| q.update());
            let shared_quotes = Arc::new(quotes.clone()); 
            let mut clients_guard = generator_server.clients_quard().unwrap();
            clients_guard.retain(|s| {
                s.send(Arc::clone(&shared_quotes)).is_ok()
            });
        }
    });

    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                let mut cmd = String::new();
                match stream.read_to_string(&mut cmd) {
                    Ok(n) if n > 0 => {
                        let mut parts = cmd.split_whitespace();
                        if let Some("STREAM") = parts.next() {
                            let address = parts.next().map(|s| s.to_string());
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
