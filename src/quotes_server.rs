use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{self, Sender, Receiver};
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
                    eprintln!("Unable to send generated data to client's channel")
                }
            });
            thread::sleep(Duration::from_secs(1));
        }
    }

    pub fn stream_quotes(&self, address: SocketAddr, tickets: HashSet<String>) { //return Result
        let (quotes_tx, quotes_rx) = mpsc::channel();
        {
            let mut clients = self.clients.write().unwrap();
            clients.insert(address, QuotesClient::new(quotes_tx));
        }
        while let Ok(quotes) = quotes_rx.recv() && self.is_active() {
            let filtered: Vec<&StockQuote> = quotes.iter().filter(|q| tickets.contains(&q.ticker)).collect();
            if !filtered.is_empty() {
                let encoded = postcard::to_stdvec(&filtered).unwrap();
                if !self.socket.send_to(&encoded, address).is_ok() {
                    break;
                }
            }
        }
    }

    pub fn ping(&self) {
        let mut buf = [0u8; 1024];
        while self.is_active() {
            match self.socket.recv_from(&mut buf) {
                Ok((amt, address)) => {
                    if String::from_utf8_lossy(&buf[..amt]).trim() == "Ping" {
                        let clients = self.clients.read().unwrap();
                        if let Some(client) = clients.get(&address) {
                            client.timestamp.store(QuotesClient::now(), Ordering::Relaxed);
                        }
                    }
                },
                Err(_) => todo!(),
            }
        }
    }

    pub fn keep_alive(&self) {
        while self.is_active() {
            let mut inactive_addresses = Vec::new();
            let now = QuotesClient::now();

            let clients = self.clients.read().unwrap();
            clients.iter().for_each(|(address, client)| {
                if now - client.timestamp.load(Ordering::Relaxed) > 5 {
                    inactive_addresses.push(address);
                }
            });

            if !inactive_addresses.is_empty() {
                let mut clients = self.clients.write().unwrap();
                for address in inactive_addresses {
                    clients.remove(address);
                }
            }

            thread::sleep(Duration::from_secs(1));
        }
    }
}
