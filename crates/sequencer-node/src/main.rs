// crates/sequencer-node/src/main.rs

use std::{
    collections::HashMap,
    fs::{self, File},
    path::Path,
    pin::Pin,
    str::FromStr,
    io::Write,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use axum::{
    extract::ws::{Message, WebSocket, WebSocketUpgrade},
    extract::State,
    response::IntoResponse,
};

use blst::min_pk::SecretKey as BlsSecretKey;
use clap::Parser;
use engine::processor::Command;
use engine::{EngineEvent, Side as EngineSide};
use libp2p::identity::{ed25519, Keypair as P2pKeypair};
use libp2p::PeerId;
use log::{error, info, warn};
use rand::{Rng, RngCore};
use rpassword::read_password;
use schnorrkel::SecretKey as SchnorrkelSecretKey;
use sequencer_node::{
    consensus::{
        ConsensusEngine, ConsensusMessage, ConsensusState,
        QuorumCertificate,
    },
    crypto::{public_key_to_address, KeyPair, ValidatorKeys},
    genesis::{Genesis, GenesisAccount},
    keystore::Keystore,
    p2p::{self, P2pCommand, SyncResponse},
    ChainMessage,
};
use tokio::{
    select,
    sync::{broadcast, mpsc, oneshot, Mutex, RwLock},
};
use tokio_stream::{wrappers::ReceiverStream, Stream};
use tonic::{transport::Server, Request, Response, Status};
use tracing_log::LogTracer;
use tracing_subscriber::{EnvFilter, FmtSubscriber};
use trading::trading_engine_server::{TradingEngine, TradingEngineServer};
use trading::{
    CancelOrderRequest, CancelOrderResponse, DepthRequest, DepthResponse, ExecutionReport,
    IntentBidRequest, IntentBidResponse, IntentBundle, OrderLevel as ProtoOrderLevel,
    PlaceOrderRequest, PlaceOrderResponse, Side as ProtoSide, TradeExecution,
};

mod settlement;

pub mod trading {
    tonic::include_proto!("trading");
}

type ConsensusMsgTuple = (ConsensusMessage, PeerId, Option<oneshot::Sender<SyncResponse>>);

#[derive(Parser, Debug)]
#[clap(version, about, long_about = None)]
struct Args {
    #[clap(long)]
    bootstrap: bool,
    #[clap(long, help = "Alamat multiaddr dari bootstrap node")]
    bootstrap_node: Vec<String>,
    #[clap(long, default_value = "50000")]
    p2p_port: u16,
    #[clap(
        long,
        help = "Hanya cetak PeerId untuk db-path yang diberikan dan keluar."
    )]
    get_peer_id: bool,
    #[clap(long)]
    dev: bool,
    #[clap(long, default_value = "./sequencer_data")]
    db_path: String,
    #[clap(long, default_value = "9000")]
    metrics_port: u16,
    #[clap(long)]
    is_authority: bool,
    #[clap(long)]
    keystore_path: Option<String>,
    #[clap(long)]
    vrf_priv_key: Option<String>,
    #[clap(long)]
    bls_private_key: Option<String>,
    #[clap(long)]
    password: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    LogTracer::init()?;
    let subscriber = FmtSubscriber::builder()
        .with_env_filter(EnvFilter::from_default_env())
        .with_target(true)
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    let args = Args::parse();

    if args.dev {
        p2p::DEV_MODE.store(true, Ordering::SeqCst);
        warn!("Menjalankan dalam mode PENGEMBANGAN. Alamat loopback P2P akan diizinkan.");
    }

    if args.get_peer_id {
        let p2p_key_path = Path::new(&args.db_path).join("p2p_keypair");
        let keypair = if p2p_key_path.exists() {
            let mut key_bytes = fs::read(&p2p_key_path)?;
            let secret_key = ed25519::SecretKey::try_from_bytes(&mut key_bytes)
                .map_err(|e| format!("File P2P keypair corrupt: {}", e))?;
            P2pKeypair::from(ed25519::Keypair::from(secret_key))
        } else {
            let ed25519_keypair = ed25519::Keypair::generate();
            fs::create_dir_all(&args.db_path)?;
            fs::write(&p2p_key_path, ed25519_keypair.secret().as_ref())
                .map_err(|e| format!("Gagal menyimpan keypair baru: {}", e))?;
            P2pKeypair::from(ed25519_keypair)
        };
        let peer_id = PeerId::from(keypair.public());
        println!("{}", peer_id);
        return Ok(());
    }

    if args.bootstrap {
        info!("Mem-bootstrap state awal dan menghasilkan genesis.json yang lengkap...");
        const NUM_VALIDATORS: usize = 7;
        let mut validator_keys_generated: Vec<ValidatorKeys> = Vec::new();
        let mut genesis_accounts = HashMap::new();
        let mut p2p_keypairs: Vec<P2pKeypair> = Vec::new();

        for _ in 0..NUM_VALIDATORS {
            validator_keys_generated.push(ValidatorKeys::new());
            p2p_keypairs.push(P2pKeypair::generate_ed25519());
        }

        for (i, keys) in validator_keys_generated.iter().enumerate() {
            let address_hex = hex::encode(keys.signing_keys.public_key_bytes());
            let bls_public_key = keys.bls_secret_key.sk_to_pk();

            let p2p_key = &p2p_keypairs[i];
            let peer_id = PeerId::from(p2p_key.public());

            let port = 50000 + i;
            let multiaddr = format!("/ip4/127.0.0.1/tcp/{}/p2p/{}", port, peer_id);

            let account = GenesisAccount {
                public_key: address_hex.clone(),
                vrf_public_key: Some(hex::encode(keys.vrf_keys.public.to_bytes())),
                bls_public_key: Some(hex::encode(bls_public_key.to_bytes())),
                network_identity: Some(multiaddr),
            };
            genesis_accounts.insert(address_hex, account);
        }

        let genesis = Genesis {
            genesis_time: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_secs(),
            chain_id: "evice-testnet-v1".to_string(),

            parameters: sequencer_node::genesis::GenesisParameters {
                aegis_sub_committee_size: 6,
                aegis_gravity_epoch_length: 10,
                proposer_timeout_ms: 1200,
            },
            accounts: genesis_accounts,
        };

        let genesis_json = serde_json::to_string_pretty(&genesis)?;
        let mut file = File::create("genesis.json")?;
        file.write_all(genesis_json.as_bytes())?;

        println!("\n========================================================================");
        println!("             KUNCI VALIDATOR GENESIS (UNTUK SCRIPT)                       ");
        println!("==========================================================================");
        for (i, keys) in validator_keys_generated.iter().enumerate() {
            let bls_public_key = keys.bls_secret_key.sk_to_pk();
            println!("\n--- Validator {} ---", i + 1);
            println!(
                "Alamat (Sign PubKey): 0x{}",
                hex::encode(keys.signing_keys.public_key_bytes())
            );
            println!(
                "Signing Private Key:  0x{}",
                hex::encode(keys.signing_keys.private_key_bytes())
            );
            println!(
                "VRF Public Key:       0x{}",
                hex::encode(keys.vrf_keys.public.to_bytes())
            );
            println!(
                "VRF Secret Key:       0x{}",
                hex::encode(keys.vrf_keys.secret.to_bytes())
            );
            println!(
                "BLS Public Key:       0x{}",
                hex::encode(bls_public_key.to_bytes())
            );
            println!(
                "BLS Secret Key:       0x{}",
                hex::encode(keys.bls_secret_key.to_bytes())
            );
        }
        println!("\n========================================================================");
        info!("Bootstrap selesai. Program berhenti.");
        std::process::exit(0);
    }

    loop {
        const CONSENSUS_START_DELAY_SECS: u64 = 45;

        let genesis = Genesis::from_file("genesis.json")
            .expect("File genesis.json tidak ditemukan atau tidak valid.");

        let chain_id = genesis.chain_id.clone();
        let genesis_time = genesis.genesis_time;

        let target_start_time =
            std::time::UNIX_EPOCH + Duration::from_secs(genesis_time + CONSENSUS_START_DELAY_SECS);
        info!(
            "[MAIN] Waktu startup node global dijadwalkan pada {:?}",
            target_start_time
        );

        let current_time = std::time::SystemTime::now();
        if let Ok(wait_duration) = target_start_time.duration_since(current_time) {
            info!(
                "[MAIN] Akan tidur selama {:?} hingga waktu startup global.",
                wait_duration
            );
            tokio::time::sleep(wait_duration).await;
        } else {
            warn!("[MAIN] Waktu startup global sudah berlalu. Memulai node segera.");
        }

        info!("[MAIN] Waktu startup global tercapai. Memulai semua layanan node...");

        let metrics_port = args.metrics_port;
        tokio::task::spawn_blocking(move || {
            info!(
                "Menjalankan server metrik di http://0.0.0.0:{}/metrics",
                metrics_port
            );
            // metrics::run_metrics_server(metrics_port);
        });

        let authority_validator_keys: Option<Arc<ValidatorKeys>> = if args.is_authority {
            let keystore_path = args
                .keystore_path
                .clone()
                .expect("Node authority harus dijalankan dengan --keystore-path");
            let vrf_priv_key_hex = args
                .vrf_priv_key
                .clone()
                .expect("Node authority harus dijalankan dengan --vrf-private-key");
            let bls_priv_key_hex = args
                .bls_private_key
                .clone()
                .expect("Node authority harus dijalankan dengan --bls-private-key");

            info!("Membuka keystore dari: {}", keystore_path);
            let keystore = Keystore::from_path(&keystore_path)?;
            let password = match args.password {
                Some(ref p) => {
                    info!("Menggunakan kata sandi yang disediakan dari argumen CLI.");
                    p
                }
                None => {
                    println!("🔒 Masukkan kata sandi untuk keystore '{}':", keystore_path);
                    &read_password()?
                }
            };
            let sk_bytes_vec = keystore.decrypt(&password)?;
            let pk_bytes = hex::decode(&keystore.public_key)?;
            let signing_keys = KeyPair::from_key_bytes(&pk_bytes, &sk_bytes_vec)?;
            let signing_address = public_key_to_address(&signing_keys.public_key_bytes());
            info!(
                "Menjalankan sebagai NODE OTORITAS dengan alamat: 0x{}",
                hex::encode(signing_address.as_ref())
            );
            let vrf_secret_bytes = hex::decode(vrf_priv_key_hex)?;
            let vrf_secret = SchnorrkelSecretKey::from_bytes(&vrf_secret_bytes)
                .map_err(|_| "VRF private key tidak valid. Pastikan panjangnya 64-byte.")?;
            let vrf_keys = vrf_secret.to_keypair();

            let mut ikm = [0u8; 32];
            rand::rng().fill_bytes(&mut ikm);
            let bls_secret_bytes = hex::decode(bls_priv_key_hex)?;
            let bls_secret_key = BlsSecretKey::from_bytes(&bls_secret_bytes)
                .map_err(|_| "BLS private key tidak valid.")?;

            Some(Arc::new(ValidatorKeys {
                signing_keys,
                vrf_keys,
                bls_secret_key,
            }))
        } else {
            None
        };

        let address_book = Arc::new(Mutex::new(p2p::AddressBook::default()));
        {
            let mut ab = address_book.lock().await;
            ab.update_from_genesis(&genesis);
            info!(
                "[MAIN] AddressBook diinisialisasi dengan {} entri dari genesis.",
                ab.get_all_peer_ids().len()
            );
        }

        let (tx_gossip, _rx_gossip) = mpsc::channel::<ChainMessage>(100);
        let (p2p_cmd_tx, p2p_cmd_rx) = mpsc::channel::<P2pCommand>(100);
        let (consensus_msg_tx, consensus_msg_rx) = mpsc::channel::<ConsensusMsgTuple>(100);

        let p2p_ready_flag = Arc::new(AtomicBool::new(false));
        let consensus_ready_flag = Arc::new(AtomicBool::new(true)); // Sequencer always ready
        let mut consensus_state: Option<Arc<RwLock<ConsensusState>>> = None;
        let (txs_response_to_consensus_tx, txs_response_from_p2p_rx) = mpsc::channel(100);

        if let Some(ref keys) = authority_validator_keys {
            let my_address = public_key_to_address(&keys.signing_keys.public_key_bytes());
            let (initial_qc, initial_block_hash) = {
                let genesis_qc = QuorumCertificate::genesis_qc();
                let genesis_hash = vec![0u8; 32];
                (genesis_qc, genesis_hash)
            };

            let state_struct = ConsensusState::new(initial_qc, initial_block_hash);
            consensus_state = Some(Arc::new(RwLock::new(state_struct.clone())));

            let engine = ConsensusEngine {
                my_address,
                validator_keys: Arc::clone(keys),
                p2p_cmd_tx: p2p_cmd_tx.clone(),
                state: state_struct.clone(),
                consensus_ready: consensus_ready_flag.clone(),
                address_book: Arc::clone(&address_book),
                pending_tx_requests: Arc::new(RwLock::new(HashMap::new())),
                tx_gossip: tx_gossip.clone(),
                chain_id: chain_id.clone(),
            };
            tokio::spawn(engine.run(consensus_msg_rx, txs_response_from_p2p_rx));
        }

        let is_bootstrap_node = args.bootstrap_node.is_empty();

        let p2p_keypair = {
            let p2p_key_path = Path::new(&args.db_path).join("p2p_keypair");

            if p2p_key_path.exists() {
                match fs::read(&p2p_key_path) {
                    Ok(mut key_bytes) => match ed25519::SecretKey::try_from_bytes(&mut key_bytes) {
                        Ok(secret_key) => {
                            let ed25519_keypair = ed25519::Keypair::from(secret_key);
                            let keypair = P2pKeypair::from(ed25519_keypair);
                            info!("P2P keypair berhasil dimuat dari: {:?}", p2p_key_path);
                            keypair
                        }
                        Err(e) => {
                            warn!("File P2P keypair corrupt, membuat yang baru. Error: {}", e);

                            let backup_path = p2p_key_path.with_extension("corrupt.bak");
                            let _ = fs::rename(&p2p_key_path, &backup_path);

                            let ed25519_keypair = ed25519::Keypair::generate();
                            fs::write(&p2p_key_path, ed25519_keypair.secret().as_ref())
                                .map_err(|e| format!("Gagal menyimpan keypair baru: {}", e))?;
                            info!("P2P keypair baru berhasil dibuat");
                            P2pKeypair::from(ed25519_keypair)
                        }
                    },
                    Err(e) => {
                        error!("Gagal membaca file P2P keypair: {}", e);
                        return Err(e.into());
                    }
                }
            } else {
                let ed25519_keypair = ed25519::Keypair::generate();
                fs::write(&p2p_key_path, ed25519_keypair.secret().as_ref())
                    .map_err(|e| format!("Gagal menyimpan keypair: {}", e))?;
                info!(
                    "P2P keypair baru berhasil dibuat dan disimpan ke: {:?}",
                    p2p_key_path
                );
                P2pKeypair::from(ed25519_keypair)
            }
        };

        let local_peer_id = PeerId::from(p2p_keypair.public());
        info!("Peer ID lokal (dari main, persisten): {}", local_peer_id);

        let bootstrap_nodes_clone = args.bootstrap_node.clone();

        if !args.bootstrap_node.is_empty() {
            let delay_ms = rand::rng().random_range(500..2000);
            info!("Node non-bootstrap, menunggu selama {}ms sebelum memulai P2P untuk memberi waktu pada bootstrap node.", delay_ms);
            tokio::time::sleep(Duration::from_millis(delay_ms)).await;
        }

        let p2p_future = p2p::run(
            p2p_keypair,
            bootstrap_nodes_clone,
            args.p2p_port,
            consensus_msg_tx,
            txs_response_to_consensus_tx,
            is_bootstrap_node,
            consensus_state,
            p2p_cmd_rx,
            p2p_cmd_tx.clone(),
            p2p_ready_flag.clone(),
            Arc::clone(&address_book),
        );

        let (tx, rx) = mpsc::channel(1024);
        let (broadcast_tx, _) = broadcast::channel(100);

        let l1_rpc_url =
            std::env::var("L1_RPC_URL").unwrap_or_else(|_| "http://127.0.0.1:8545".to_string());

        let private_key = std::env::var("SEQUENCER_PRIVATE_KEY").unwrap_or_else(|_| {
            "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80".to_string()
        });

        let rollup_address = std::env::var("ROLLUP_CONTRACT_ADDRESS")
            .unwrap_or_else(|_| "0x5FbDB2315678afecb367f032d93F642f64180aa3".to_string());

        let settlement_engine = Arc::new(
            settlement::SettlementEngine::new(l1_rpc_url, private_key, rollup_address).await,
        );

        let (outbound_tx, mut outbound_rx) = tokio::sync::mpsc::channel(1024);

        let processor_broadcast_tx = broadcast_tx.clone();
        let mut processor =
            engine::processor::MarketProcessor::new(rx, processor_broadcast_tx, outbound_tx);
        tokio::spawn(async move {
            processor.run().await;
        });

        let engine_clone = settlement_engine.clone();
        tokio::spawn(async move {
            info!("Settlement Background Task listening for OFA events...");

            while let Some(event) = outbound_rx.recv().await {
                match event {
                    EngineEvent::IntentResolved {
                        intent_id,
                        winning_solver: _,
                        winning_amount: _,
                    } => {
                        info!("Meneruskan Intent {} ke Ethereum L1...", intent_id);
                        let dummy_state_root = [0u8; 32];
                        let dummy_proof = vec![1, 2, 3, 4, 5];

                        let intent_bytes = alloy_primitives::FixedBytes::<32>::from_str(&intent_id)
                            .unwrap_or(alloy_primitives::FixedBytes::<32>::ZERO);

                        let batch = settlement::SettlementBatch {
                            new_state_root: dummy_state_root,
                            proof: dummy_proof,
                            resolved_intent_ids: vec![intent_bytes.0],
                        };

                        engine_clone.submit_zk_batch(batch).await;
                    }
                    _ => { /* Abaikan event lain seperti OrderPlaced */ }
                }
            }
        });

        let app = axum::Router::new()
            .route("/ws", axum::routing::get(ws_handler))
            .with_state(broadcast_tx);

        let ws_addr = std::net::SocketAddr::from(([127, 0, 0, 1], 3000));
        info!(">>> WebSocket Market Data Server Listening on ws://127.0.0.1:3000/ws");

        tokio::spawn(async move {
            let listener = tokio::net::TcpListener::bind(ws_addr).await.unwrap();
            axum::serve(listener, app).await.unwrap();
        });

        let addr = "[::1]:50051".parse()?;
        let trading_service = TradingService {
            processor_sender: tx,
        };

        info!("DEX Engine listening on {}", addr);

        let grpc_future = Server::builder()
            .add_service(TradingEngineServer::new(trading_service))
            .serve(addr);

        select! {
            res = p2p_future => {
                if let Err(e) = res {
                    error!("P2P Future exited with error: {}", e);
                }
                break;
            },
            res = grpc_future => {
                if let Err(e) = res {
                    error!("gRPC Future exited with error: {}", e);
                }
                break;
            },
            _ = tokio::signal::ctrl_c() => {
                info!("Menerima sinyal Ctrl-C, node berhenti.");
                break;
            },

        }
    }

    Ok(())
}

pub struct TradingService {
    processor_sender: mpsc::Sender<Command>,
}

#[tonic::async_trait]
impl TradingEngine for TradingService {
    async fn place_limit_order(
        &self,
        request: Request<PlaceOrderRequest>,
    ) -> Result<Response<PlaceOrderResponse>, Status> {
        let req = request.into_inner();

        // Validasi & Konversi Input (Proto -> Internal)
        let side = match ProtoSide::try_from(req.side).unwrap_or(ProtoSide::Unspecified) {
            ProtoSide::Bid => EngineSide::Bid,
            ProtoSide::Ask => EngineSide::Ask,
            ProtoSide::Unspecified => return Err(Status::invalid_argument("Side is required")),
        };

        let (resp_tx, resp_rx) = oneshot::channel();

        let command = Command::PlaceOrder {
            user_id: req.user_id,
            order_id: req.order_id,
            side,
            price: req.price,
            quantity: req.quantity,
            responder: resp_tx,
        };

        self.processor_sender
            .send(command)
            .await
            .map_err(|_| Status::internal("Engine is down"))?;

        let events = resp_rx
            .await
            .map_err(|_| Status::internal("Engine failed to respond"))?;

        let mut fills = Vec::new();
        let mut success = false;

        for event in events {
            match event {
                EngineEvent::OrderPlaced { id, .. } if id == req.order_id => {
                    success = true;
                }
                EngineEvent::TradeExecuted {
                    maker_id,
                    taker_id,
                    price,
                    quantity,
                } => {
                    if taker_id == req.order_id {
                        fills.push(TradeExecution {
                            maker_order_id: maker_id,
                            price,
                            quantity,
                        });
                        success = true;
                    }
                }
                EngineEvent::OrderCancelled { .. } => {}
                _ => {}
            }
        }

        Ok(Response::new(PlaceOrderResponse {
            success,
            message: if success {
                "Order Processed".to_string()
            } else {
                "Order Rejected".to_string()
            },
            fills,
        }))
    }

    async fn execute_solver_bundle(
        &self,
        request: Request<IntentBundle>,
    ) -> Result<Response<ExecutionReport>, Status> {
        let req = request.into_inner();

        // Konversi Proto Orders ke Internal Command Orders
        let mut bundle_orders = Vec::new();

        for order in req.orders {
            let side = match ProtoSide::try_from(order.side).unwrap_or(ProtoSide::Unspecified) {
                ProtoSide::Bid => EngineSide::Bid,
                ProtoSide::Ask => EngineSide::Ask,
                ProtoSide::Unspecified => return Err(Status::invalid_argument("Side Invalid")),
            };

            bundle_orders.push(engine::processor::BundleRequest {
                user_id: order.user_id,
                order_id: order.order_id,
                side,
                price: order.price,
                quantity: order.quantity,
            });
        }

        let (resp_tx, resp_rx) = oneshot::channel();

        self.processor_sender
            .send(Command::ExecuteBundle {
                orders: bundle_orders,
                responder: resp_tx,
            })
            .await
            .map_err(|_| Status::internal("Engine is down"))?;

        let events = resp_rx
            .await
            .map_err(|_| Status::internal("Engine faield to respond"))?;

        // Buat Laporan Eksekusi (ExecutionReport)
        // A. Filter dan Map event menjadi TradeExecution
        let fills: Vec<TradeExecution> = events
            .iter()
            .filter_map(|event| {
                match event {
                    // Kita hanya peduli event TradeExecuted
                    engine::EngineEvent::TradeExecuted {
                        maker_id,
                        price,
                        quantity,
                        ..
                    } => Some(TradeExecution {
                        maker_order_id: *maker_id,
                        price: *price,
                        quantity: *quantity,
                    }),
                    _ => None,
                }
            })
            .collect();

        // B. Return Response
        Ok(Response::new(ExecutionReport {
            success: true,
            message: format!("Bundle Executed. Total Events: {}", events.len()),
            fills,
        }))
    }

    async fn cancel_order(
        &self,
        request: Request<CancelOrderRequest>,
    ) -> Result<Response<CancelOrderResponse>, Status> {
        let req = request.into_inner();
        let (resp_tx, resp_rx) = oneshot::channel();

        self.processor_sender
            .send(Command::CancelOrder {
                user_id: req.user_id,
                order_id: req.order_id,
                responder: resp_tx,
            })
            .await
            .map_err(|_| Status::internal("Engine down"))?;

        let events = resp_rx.await.map_err(|_| Status::internal("No response"))?;

        let success = events
            .iter()
            .any(|e| matches!(e, EngineEvent::OrderCancelled { .. }));

        Ok(Response::new(CancelOrderResponse {
            success,
            remaining_qty: 0,
        }))
    }

    async fn get_order_book_depth(
        &self,
        request: Request<DepthRequest>,
    ) -> Result<Response<DepthResponse>, Status> {
        let req = request.into_inner();
        let limit = if req.limit == 0 {
            10
        } else {
            req.limit as usize
        };

        let (resp_tx, resp_rx) = oneshot::channel();

        self.processor_sender
            .send(Command::GetDepth {
                limit,
                responder: resp_tx,
            })
            .await
            .map_err(|_| Status::internal("Engine down"))?;

        let (asks, bids) = resp_rx.await.map_err(|_| Status::internal("No response"))?;

        let proto_asks = asks
            .into_iter()
            .map(|l| ProtoOrderLevel {
                price: l.price,
                total_quantity: l.quantity,
            })
            .collect();

        let proto_bids = bids
            .into_iter()
            .map(|l| ProtoOrderLevel {
                price: l.price,
                total_quantity: l.quantity,
            })
            .collect();

        Ok(Response::new(DepthResponse {
            bids: proto_bids,
            asks: proto_asks,
            sequence_id: 0,
        }))
    }

    async fn submit_intent_bid(
        &self,
        request: Request<IntentBidRequest>,
    ) -> Result<Response<IntentBidResponse>, Status> {
        let req = request.into_inner();

        // Validasi Bid (Tanda tangan, format)
        if req.solver_signature.is_empty() {
            return Err(Status::invalid_argument("Signature required"));
        }

        let (resp_tx, _resp_rx) = oneshot::channel();

        // Kirim Bid ke Engine/Processor yang baru
        self.processor_sender
            .send(Command::SubmitBid {
                solver_id: req.solver_id,
                intent_id: req.intent_id.clone(),
                proposed_output_amount: req.proposed_output_amount,
                estimated_gas_cost: req.estimated_gas_cost,
                solver_signature: req.solver_signature,
                responder: resp_tx,
            })
            .await
            .map_err(|_| Status::internal("Engine is down"))?;

        Ok(Response::new(IntentBidResponse {
            accepted: true,
            message: "Bid queued for OFA evaluation".to_string(),
            auction_id: format!("auc-{}", req.intent_id),
        }))
    }

    type SubscribeIntentMempoolStream =
        Pin<Box<dyn Stream<Item = Result<trading::IntentEvent, Status>> + Send>>;

    async fn subscribe_intent_mempool(
        &self,
        request: Request<trading::MempoolSubscribeRequest>,
    ) -> Result<Response<Self::SubscribeIntentMempoolStream>, Status> {
        let _req = request.into_inner();

        // Logika penyambungan asli ke Mempool L2 akan dilakukan di sini nanti
        let (_, rx) = tokio::sync::mpsc::channel(128);
        let output_stream = ReceiverStream::new(rx);

        Ok(Response::new(
            Box::pin(output_stream) as Self::SubscribeIntentMempoolStream
        ))
    }
}

async fn ws_handler(
    ws: WebSocketUpgrade,
    State(broadcast_tx): State<broadcast::Sender<EngineEvent>>,
) -> impl IntoResponse {
    ws.on_upgrade(|socket| handle_socket(socket, broadcast_tx))
}

async fn handle_socket(mut socket: WebSocket, broadcast_tx: broadcast::Sender<EngineEvent>) {
    let mut rx = broadcast_tx.subscribe();

    while let Ok(event) = rx.recv().await {
        // Konversi EngineEvent ke JSON
        let json_msg = match event {
            EngineEvent::TradeExecuted {
                maker_id,
                taker_id,
                price,
                quantity,
            } => serde_json::json! ({
                "type": "TRADE",
                "maker_id": maker_id,
                "taker_id": taker_id,
                "price": price,
                "quantity": quantity,
            }),
            EngineEvent::OrderPlaced {
                id,
                price,
                quantity,
                side,
                ..
            } => serde_json::json! ({
                "type": "ORDER_PLACED",
                "id": id,
                "price": price,
                "quantity": quantity,
                "side": format!("{:?}", side),
            }),
            EngineEvent::OrderCancelled { id } => serde_json::json! ({
                "type": "ORDER_CANCELLED",
                "id": id,
            }),
            EngineEvent::IntentResolved {
                intent_id,
                winning_solver,
                winning_amount,
            } => serde_json::json!({
                "type": "INTENT_RESOLVED",
                "intent_id": intent_id,
                "winning_solver": winning_solver,
                "winning_amount": winning_amount,
            }),
        };

        if let Ok(msg_text) = serde_json::to_string(&json_msg) {
            if socket.send(Message::Text(msg_text)).await.is_err() {
                break;
            }
        }
    }
}
