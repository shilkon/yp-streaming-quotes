use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{self, Sender};
use std::net::{SocketAddr, UdpSocket};
use std::{io, thread};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::StockQuote;

pub struct QuotesClient {
    pub sender: Sender<Arc<Vec<StockQuote>>>,
    pub timestamp: AtomicU64
}

impl QuotesClient {
    pub fn new(sender: Sender<Arc<Vec<StockQuote>>>) -> QuotesClient {
        QuotesClient{
            sender,
            timestamp: AtomicU64::new(QuotesClient::now())
        }
    }

    fn now() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
    }
}

pub struct QuotesServer {
    is_active: AtomicBool,
    clients: RwLock<HashMap<SocketAddr, QuotesClient>>,
    socket: UdpSocket
}

impl QuotesServer {
    const GENERATOR_TIMEOUT: u64 = 1;
    const CLIENT_TIMEOUT: u64 = 5;
    const KEEP_ALIVE_TIMEOUT: u64 = 5;
    pub const UDP_MESSAGE_BUFFER_SIZE: usize = 1024;

    pub fn new(server_address: &str) -> io::Result<QuotesServer> {
        Ok(QuotesServer {
            is_active: AtomicBool::new(true),
            clients: RwLock::new(HashMap::new()),
            socket: UdpSocket::bind(server_address)?
        })
    }

    pub fn is_active(&self) -> bool {
        self.is_active.load(Ordering::Relaxed)
    }

    pub fn generate_qoutes(&self, mut quotes: Vec<StockQuote>) {
        while self.is_active() {
            quotes.iter_mut().for_each(|q| q.update());
            let shared_quotes = Arc::new(quotes.clone()); 
            let clients_guard = self.clients.read().unwrap();
            clients_guard.iter().for_each(|(_, client)| {
                if let Err(e) = client.sender.send(Arc::clone(&shared_quotes)) {
                    log::error!("Error while sending generated data to client's channel: {e}")
                }
            });
            thread::sleep(Duration::from_secs(Self::GENERATOR_TIMEOUT));
        }

        log::info!("Quotes generation ended");
    }

    pub fn stream_quotes(&self, address: SocketAddr, tickers: HashSet<String>) { //return Result
        log::info!("Starting streaming quotes for '{}' with tickers: {}", address,
            tickers.iter().map(|s| s.as_str()).collect::<Vec<&str>>().join(", "));

        let (quotes_tx, quotes_rx) = mpsc::channel();
        {
            let mut clients = self.clients.write().unwrap();
            clients.insert(address, QuotesClient::new(quotes_tx));
        }

        while let Ok(quotes) = quotes_rx.recv() && self.is_active() {
            let filtered: Vec<&StockQuote> = quotes.iter().filter(|q| tickers.contains(&q.ticker)).collect();
            if !filtered.is_empty() {
                let encoded = postcard::to_stdvec(&filtered).unwrap();
                match self.socket.send_to(&encoded, address) {
                    Ok(_) => log::info!("Sended data to '{address}'"),
                    Err(e) => log::warn!("Error while sending data to '{address}': {e}")
                }
            }
        }

        log::info!("Streaming quotes for '{address}' ended");
    }

    pub fn ping(&self) {
        let mut buf = [0u8; Self::UDP_MESSAGE_BUFFER_SIZE];
        while self.is_active() {
            match self.socket.recv_from(&mut buf) {
                Ok((amt, address)) => {
                    match String::from_utf8_lossy(&buf[..amt]).trim() {
                        "Ping" => {
                            let clients = self.clients.read().unwrap();
                            if let Some(client) = clients.get(&address) {
                                client.timestamp.store(QuotesClient::now(), Ordering::Relaxed);
                            } else {
                                log::warn!("Unknown client '{address}'");
                            }
                        }
                        s => log::warn!("Unsupported command '{s}' instead of 'Ping'")
                    }
                },
                Err(e) => log::warn!("Didn't recieve ping request: {e}"),
            }
        }

        log::info!("Ping request processing ended");
    }

    pub fn keep_alive(&self) {
        while self.is_active() {
            let mut addresses = Vec::new();
            let now = QuotesClient::now();

            let clients = self.clients.read().unwrap();
            clients.iter().for_each(|(address, client)| {
                if now - client.timestamp.load(Ordering::Relaxed) > Self::CLIENT_TIMEOUT {
                    log::warn!("Connection timed out for '{address}'");
                    addresses.push(address);
                }
            });

            if !addresses.is_empty() {
                let inactive_addresses: Vec<SocketAddr> = addresses.into_iter().copied().collect();
                drop(clients);

                let mut clients = self.clients.write().unwrap();
                for address in inactive_addresses {
                    clients.remove(&address);
                }
            }

            thread::sleep(Duration::from_secs(Self::KEEP_ALIVE_TIMEOUT));
        }

        log::info!("Keeping alive clients ended");
    }
}
