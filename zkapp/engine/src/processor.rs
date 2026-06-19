// crates/engine-core/src/processor.rs

use std::collections::{BTreeMap, HashMap};
use tokio::{
    sync::{broadcast, mpsc},
    time::{interval, Duration},
};
use tracing::{error, info, warn};

use crate::wal::WalHandler;
use crate::{EngineEvent, LogEntry, OrderBook, OrderLevel, Side};

#[derive(Debug)]
pub struct BundleRequest {
    pub user_id: u64,
    pub order_id: u64,
    pub side: Side,
    pub price: u64,
    pub quantity: u64,
}

#[derive(Debug)]
pub enum Command {
    SubmitBid {
        solver_id: String,
        intent_id: String,
        proposed_output_amount: u64,
        estimated_gas_cost: u64,
        solver_signature: Vec<u8>,
        responder: tokio::sync::oneshot::Sender<Result<String, String>>,
    },
    PlaceOrder {
        user_id: u64,
        order_id: u64,
        side: Side,
        price: u64,
        quantity: u64,
        responder: tokio::sync::oneshot::Sender<Vec<EngineEvent>>,
    },
    ExecuteBundle {
        orders: Vec<BundleRequest>,
        responder: tokio::sync::oneshot::Sender<Vec<EngineEvent>>,
    },
    CancelOrder {
        user_id: u64,
        order_id: u64,
        responder: tokio::sync::oneshot::Sender<Vec<EngineEvent>>,
    },
    GetDepth {
        limit: usize,
        responder: tokio::sync::oneshot::Sender<(Vec<OrderLevel>, Vec<OrderLevel>)>,
    },
}

#[derive(Debug, Clone)]
struct Auction {
    intent_id: String,
    bids: BTreeMap<u64, String>,
    end_time: std::time::Instant,
}
pub struct MarketProcessor {
    book: OrderBook,
    receiver: mpsc::Receiver<Command>,
    wal: WalHandler,
    active_auctions: HashMap<String, Auction>,
    outbound_events: mpsc::Sender<EngineEvent>,
    pub event_broadcaster: broadcast::Sender<EngineEvent>,
}

impl MarketProcessor {
    pub fn new(
        receiver: mpsc::Receiver<Command>,
        broadcaster: broadcast::Sender<EngineEvent>,
        outbound_events: mpsc::Sender<EngineEvent>,
    ) -> Self {
        let wal_path = "velocity.wal";

        // Recovery Phase
        info!("Recovering state from WAL...");
        let mut book = OrderBook::new();

        if let Ok(entries) = WalHandler::read_all(wal_path) {
            info!("Replaying {} events...", entries.len());
            for entry in entries {
                match entry {
                    LogEntry::Place {
                        order_id,
                        user_id,
                        side,
                        price,
                        quantity,
                    } => {
                        book.place_limit_order(order_id, user_id, side, price, quantity);
                    }
                    LogEntry::Cancel { order_id, user_id } => {
                        book.cancel_order(order_id, user_id);
                    }
                }
            }
        } else {
            warn!("No WAL found, starting fresh.");
        }

        // Open WAL for Writing
        let wal = WalHandler::new(wal_path).expect("Failed to open WAL file");

        Self {
            book,
            receiver,
            wal,
            active_auctions: HashMap::new(),
            outbound_events,
            event_broadcaster: broadcaster,
        }
    }

    pub async fn run(&mut self) {
        let mut tick = interval(Duration::from_millis(500));

        loop {
            tokio::select! {
                Some(cmd) = self.receiver.recv() => {
                    self.handle_command(cmd).await;
                }
                _ = tick.tick() => {
                    self.resolve_auctions().await;
                }
            }
        }
    }

    async fn handle_command(&mut self, cmd: Command) {
        match cmd {
            Command::SubmitBid {
                solver_id,
                intent_id,
                proposed_output_amount,
                ..
            } => {
                if let Some(auction) = self.active_auctions.get_mut(&intent_id) {
                    auction.bids.insert(proposed_output_amount, solver_id);
                } else {
                    let mut new_auction = Auction {
                        intent_id: intent_id.clone(),
                        bids: BTreeMap::new(),
                        end_time: std::time::Instant::now() + Duration::from_millis(500),
                    };
                    new_auction.bids.insert(proposed_output_amount, solver_id);
                    self.active_auctions.insert(intent_id, new_auction);
                }
            }

            Command::PlaceOrder {
                user_id,
                order_id,
                side,
                price,
                quantity,
                responder,
            } => {
                // (WAL) Persistence First (Write-Ahead)
                let log_entry = LogEntry::Place {
                    order_id,
                    user_id,
                    side,
                    price,
                    quantity,
                };

                if let Err(e) = self.wal.write_entry(&log_entry) {
                    error!("CRITICAL: Failed to write to WAL: {}", e);
                }

                // Mmemory Execution
                let events = self
                    .book
                    .place_limit_order(order_id, user_id, side, price, quantity);

                // Broadcast (Pub/Sub)
                // Kirim copy event ke semua subscriber WebSocket
                for event in &events {
                    // Hanya broadcast event publik (Trade). Private info (OrderPlaced) opsional.
                    // Di sini broadcast semuanya agar dashboard terlihat hidup
                    let _ = self.event_broadcaster.send(event.clone());
                }
                let _ = responder.send(events);
            }

            Command::ExecuteBundle { orders, responder } => {
                // Transactional Loop
                // Dalam model Single-Threaded Actor ini, atomicity dijamin
                // karena tidak ada perintah lain yang bisa menyela loop ini.
                let mut bundle_events = Vec::new();

                for req in orders {
                    // A. Write to WAL (Persistence)
                    let log_entry = LogEntry::Place {
                        order_id: req.order_id,
                        user_id: req.user_id,
                        side: req.side,
                        price: req.price,
                        quantity: req.quantity,
                    };

                    if let Err(e) = self.wal.write_entry(&log_entry) {
                        error!("WAL Write Error: {}", e);
                    }

                    // B. Execute in Memory
                    let mut events = self.book.place_limit_order(
                        req.order_id,
                        req.user_id,
                        req.side,
                        req.price,
                        req.quantity,
                    );

                    // C. Collect Events
                    // Menggabungkan event dari semua order dalam bundle
                    bundle_events.append(&mut events);
                }

                for event in &bundle_events {
                    let _ = self.event_broadcaster.send(event.clone());
                }

                let _ = responder.send(bundle_events);
            }

            Command::CancelOrder {
                user_id,
                order_id,
                responder,
            } => {
                // Persistence First
                let log_entry = LogEntry::Cancel { order_id, user_id };

                if let Err(e) = self.wal.write_entry(&log_entry) {
                    error!("CRITICAL: Failed to write to WAL: {}", e);
                }

                let events = self.book.cancel_order(order_id, user_id);

                for event in &events {
                    let _ = self.event_broadcaster.send(event.clone());
                }

                let _ = responder.send(events);
            }

            Command::GetDepth { limit, responder } => {
                // Read-only command tidak perlu ditulis ke WAL
                let depth = self.book.get_depth(limit);
                let _ = responder.send(depth);
            }
        }
    }

    async fn resolve_auctions(&mut self) {
        let now = std::time::Instant::now();
        let mut completed_auctions = Vec::new();

        self.active_auctions.retain(|_id, auction| {
            if now >= auction.end_time {
                completed_auctions.push(auction.clone());
                false
            } else {
                true
            }
        });

        for auction in completed_auctions {
            if let Some((winning_amount, winning_solver)) = auction.bids.iter().rev().next() {
                info!(
                    "Auction Won! Intent: {}, Solver: {}, Amount: {}",
                    auction.intent_id, winning_solver, winning_amount
                );

                // Di sini kita bisa mengirimkan transaksi penyelesaian ke State Machine
                // Untuk dikemas dalam batch dan dikirim ke ZK-Prover
                self.commit_to_state_machine(&auction.intent_id, winning_solver, *winning_amount)
                    .await;
            }
        }
    }

    async fn commit_to_state_machine(
        &mut self,
        intent_id: &str,
        winning_solver: &str,
        winning_amount: u64,
    ) {
        info!(
            "[OFA SETTLEMENT] Lelang Dimenangkan! Intent: {}, Winner: {}, Output: {}",
            intent_id, winning_solver, winning_amount
        );

        let event = EngineEvent::IntentResolved {
            intent_id: intent_id.to_string(),
            winning_solver: winning_solver.to_string(),
            winning_amount,
        };

        if let Err(e) = self.outbound_events.send(event).await {
            error!("Gagal mengirim event IntentResolved ke L1 Task: {}", e);
        }
    }
}
