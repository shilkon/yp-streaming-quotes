use std::collections::HashSet;
use std::sync::{Arc, Mutex, PoisonError};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Sender, Receiver};
use std::net::UdpSocket;
use std::io;

use crate::stock_quote::StockQuote;

type QuoteClient = Sender<Arc<Vec<StockQuote>>>;

pub struct QuotesServer {
    is_active: AtomicBool,
    clients: Mutex<Vec<QuoteClient>>,
    socket: UdpSocket
}

impl QuotesServer {
    pub fn new(server_address: &str) -> io::Result<QuotesServer> {
        Ok(QuotesServer {
            is_active: AtomicBool::new(true),
            clients: Mutex::new(Vec::new()),
            socket: UdpSocket::bind(server_address)?
        })
    }

    pub fn is_active(&self) -> bool {
        self.is_active.load(Ordering::Relaxed)
    }

    pub fn clients_quard(&self) -> Result<std::sync::MutexGuard<'_, Vec<QuoteClient>>,
        PoisonError<std::sync::MutexGuard<'_, Vec<QuoteClient>>>> {
        self.clients.lock()
    }

    pub fn stream_quotes(&self, address: String, tickets: HashSet<String>) { //return Result
        let (quotes_tx, quotes_rx) = mpsc::channel();
        self.clients_quard().unwrap().push(quotes_tx);
        while let Ok(quotes) = quotes_rx.recv() && self.is_active() {
            let filtered: Vec<&StockQuote> = quotes.iter().filter(|q| tickets.contains(&q.ticker)).collect();
            if !filtered.is_empty() {
                let encoded = postcard::to_stdvec(&filtered).unwrap();
                if !self.socket.send_to(&encoded, address.as_str()).is_ok() {
                    break;
                }
            }
        }
    }
}
