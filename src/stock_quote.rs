use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StockQuote {
    pub ticker: String,
    price: u64,
    volume: u32,
    timestamp: u128
}

impl StockQuote {
    pub fn new(ticker: &str) -> StockQuote {
        StockQuote{
            ticker: ticker.to_string(),
            price: rand::random::<u64>() % 1000,
            volume: 0,
            timestamp: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis()
        }
    }

    pub fn update(&mut self) -> () {
        self.volume = match self.ticker.as_str() {
            // Популярные акции имеют больший объём
            "AAPL" | "MSFT" | "TSLA" => 1000 + (rand::random::<f64>() * 5000.0) as u32,
            // Обычные акции - средний объём
            _ => 100 + (rand::random::<f64>() * 1000.0) as u32,
        };

        let price_change_trend = (rand::random::<u32>() % 3) as i8 - 1;
        if price_change_trend != 0 {
            let price_change = (rand::random::<u32>() % 1000 * self.volume) as u64;
            if price_change_trend > 0 {
                self.price += price_change;
            } else if self.price >= price_change  {
                self.price -= price_change;
            }
        }

        self.timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis();
    }
}
