use serde::{Deserialize, Serialize};
use slab::Slab;
use std::collections::{BTreeMap, HashMap, VecDeque};

pub mod processor;
pub mod wal;

pub type OrderId = u64;
pub type UserId = u64;
pub type Price = u64;
pub type Quantity = u64;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Side {
    Bid,
    Ask,
}

impl Side {
    pub fn opposite(&self) -> Self {
        match self {
            Side::Bid => Side::Ask,
            Side::Ask => Side::Bid,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Order {
    pub id: OrderId,
    pub user_id: UserId,
    pub price: Price,
    pub quantity: Quantity,
    pub side: Side,
    pub timestamp: u64,
}

#[derive(Debug, Clone)]
pub enum EngineEvent {
    OrderPlaced {
        id: OrderId,
        user_id: UserId,
        price: Price,
        quantity: Quantity,
        side: Side,
    },
    OrderCancelled {
        id: OrderId,
    },
    TradeExecuted {
        maker_id: OrderId,
        taker_id: OrderId,
        price: Price,
        quantity: Quantity,
    },
    IntentResolved {
        intent_id: String,
        winning_solver: String,
        winning_amount: u64,
    },
}

#[derive(Debug, Clone)]
pub struct OrderLevel {
    pub price: Price,
    pub quantity: Quantity,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LogEntry {
    Place {
        order_id: OrderId,
        user_id: UserId,
        side: Side,
        price: Price,
        quantity: Quantity,
    },
    Cancel {
        order_id: OrderId,
        user_id: UserId,
    },
}

pub struct OrderBook {
    order_store: Slab<Order>,
    bids: BTreeMap<Price, VecDeque<usize>>,
    asks: BTreeMap<Price, VecDeque<usize>>,
    order_index: HashMap<OrderId, usize>,
    #[allow(dead_code)]
    sequence: u64,
}

impl OrderBook {
    pub fn new() -> Self {
        Self {
            order_store: Slab::with_capacity(10_000),
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            order_index: HashMap::new(),
            sequence: 0,
        }
    }

    // Fungsi utama untuk memproses Limit Order
    // Mengembalikan daftar event yang terjadi (Trade, Placement, dll)
    pub fn place_limit_order(
        &mut self,
        order_id: OrderId,
        user_id: UserId,
        side: Side,
        price: Price,
        mut quantity: Quantity,
    ) -> Vec<EngineEvent> {
        let mut events = Vec::new();

        // Matching Process (Taker Phase)
        loop {
            if quantity == 0 {
                break;
            }

            let best_match_option = match side {
                Side::Bid => self.asks.iter_mut().next(),
                Side::Ask => self.bids.iter_mut().next_back(),
            };

            let (best_price, order_queue) = match best_match_option {
                Some((p, q)) => (*p, q),
                None => break,
            };

            let is_matchable = match side {
                Side::Bid => best_price <= price,
                Side::Ask => best_price >= price,
            };

            if !is_matchable {
                break;
            }

            while let Some(&maker_idx) = order_queue.front() {
                let maker_order = self
                    .order_store
                    .get_mut(maker_idx)
                    .expect("Stale index in queue");

                if maker_order.user_id == user_id {
                    order_queue.pop_front();
                    events.push(EngineEvent::OrderCancelled { id: maker_order.id });
                    self.order_store.remove(maker_idx);
                    continue;
                }

                let trade_qty = std::cmp::min(quantity, maker_order.quantity);

                events.push(EngineEvent::TradeExecuted {
                    maker_id: maker_order.id,
                    taker_id: order_id,
                    price: best_price,
                    quantity: trade_qty,
                });

                quantity -= trade_qty;
                maker_order.quantity -= trade_qty;

                if maker_order.quantity == 0 {
                    order_queue.pop_front();
                    self.order_store.remove(maker_idx);
                }

                if quantity == 0 {
                    break;
                }
            }

            if order_queue.is_empty() {
                match side {
                    Side::Bid => {
                        self.asks.remove(&best_price);
                    }
                    Side::Ask => {
                        self.bids.remove(&best_price);
                    }
                }
            }
        }

        // Placement Process (Maker Phase)
        if quantity > 0 {
            let new_order = Order {
                id: order_id,
                user_id,
                price,
                quantity,
                side,
                timestamp: 0,
            };

            let idx = self.order_store.insert(new_order.clone());

            self.order_index.insert(order_id, idx);

            let queue = match side {
                Side::Bid => self.bids.entry(price).or_insert_with(VecDeque::new),
                Side::Ask => self.asks.entry(price).or_insert_with(VecDeque::new),
            };
            queue.push_back(idx);

            events.push(EngineEvent::OrderPlaced {
                id: order_id,
                user_id,
                price,
                quantity,
                side,
            });
        }

        events
    }

    pub fn cancel_order(&mut self, order_id: OrderId, user_id: UserId) -> Vec<EngineEvent> {
        let mut events = Vec::new();

        if let Some(&internal_idx) = self.order_index.get(&order_id) {
            // Ambil referensi order untuk validasi
            // Gunakan get dulu, jangan remove, karena perlu cek user_id
            if let Some(order) = self.order_store.get(internal_idx) {
                // Security Check: Apakah ini order milik user yang request?
                if order.user_id != user_id {
                    return events;
                }

                let price = order.price;
                let side = order.side;
                let _remaining_qty = order.quantity;

                let queue = match side {
                    Side::Bid => self.bids.get_mut(&price),
                    Side::Ask => self.asks.get_mut(&price),
                };

                if let Some(q) = queue {
                    // O(N) operation pada queue specific price level
                    // Ini acceptable karena biasanya satu level harga tidak memiliki jutaan order
                    // retain adalah cara terbersih menghapus item tertentu
                    q.retain(|&idx| idx != internal_idx);

                    // Jika queue kosong, hapus entry harga dari BTreeMap agar hemat memori
                    if q.is_empty() {
                        match side {
                            Side::Bid => {
                                self.bids.remove(&price);
                            }
                            Side::Ask => {
                                self.asks.remove(&price);
                            }
                        }
                    }
                }

                self.order_index.remove(&order_id);
                self.order_store.remove(internal_idx);
                events.push(EngineEvent::OrderCancelled { id: order_id });
            }
        }

        events
    }

    pub fn get_depth(&self, limit: usize) -> (Vec<OrderLevel>, Vec<OrderLevel>) {
        // Ambil Asks (Jual) - Urut dari termurah (Ascending)
        let asks: Vec<OrderLevel> = self
            .asks
            .iter()
            .take(limit)
            .map(|(&price, queue)| {
                let total_qty: u64 = queue
                    .iter()
                    .map(|&idx| self.order_store.get(idx).map(|o| o.quantity).unwrap_or(0))
                    .sum();
                OrderLevel {
                    price,
                    quantity: total_qty,
                }
            })
            .collect();

        // Ambil Bids (Beli) - Urut dari termahal (Descending/Reverse)
        let bids: Vec<OrderLevel> = self
            .bids
            .iter()
            .rev()
            .take(limit)
            .map(|(&price, queue)| {
                let total_qty: u64 = queue
                    .iter()
                    .map(|&idx| self.order_store.get(idx).map(|o| o.quantity).unwrap_or(0))
                    .sum();
                OrderLevel {
                    price,
                    quantity: total_qty,
                }
            })
            .collect();

        (asks, bids)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_limit_order_placement_no_match() {
        let mut book = OrderBook::new();
        let events = book.place_limit_order(1, 1, Side::Bid, 100, 10);

        assert_eq!(events.len(), 1);
        if let EngineEvent::OrderPlaced { id, .. } = events[0] {
            assert_eq!(id, 1);
        } else {
            panic!("Event salah!");
        }
    }

    #[test]
    fn test_full_match_execution() {
        let mut book = OrderBook::new();
        book.place_limit_order(1, 1, Side::Ask, 100, 10);
        let events = book.place_limit_order(2, 2, Side::Bid, 100, 10);

        let trade_event = events
            .iter()
            .find(|e| matches!(e, EngineEvent::TradeExecuted { .. }));

        if let EngineEvent::TradeExecuted {
            maker_id,
            taker_id,
            price,
            quantity,
        } = trade_event.unwrap()
        {
            assert_eq!(*maker_id, 1);
            assert_eq!(*taker_id, 2);
            assert_eq!(*price, 100);
            assert_eq!(*quantity, 10);
        }
    }

    #[test]
    fn test_partial_match() {
        let mut book = OrderBook::new();
        book.place_limit_order(1, 1, Side::Ask, 100, 20);
        let events = book.place_limit_order(2, 2, Side::Bid, 100, 10);

        assert!(events
            .iter()
            .any(|e| matches!(e, EngineEvent::TradeExecuted { quantity: 10, .. })));
    }

    #[test]
    fn test_self_trade_prevention_cancel_maker() {
        let mut book = OrderBook::new();

        book.place_limit_order(100, 1, Side::Ask, 100, 10);
        let events = book.place_limit_order(200, 1, Side::Bid, 100, 10);

        let cancel_event = events
            .iter()
            .find(|e| matches!(e, EngineEvent::OrderCancelled { .. }));
        assert!(cancel_event.is_some(), "Harusnya ada event cancel maker");

        let place_event = events
            .iter()
            .find(|e| matches!(e, EngineEvent::OrderPlaced { .. }));
        assert!(place_event.is_some(), "Taker order harusnya masuk book");
    }
}
