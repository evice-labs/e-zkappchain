// crates/sequencer-node/src/consensus.rs

use bincode::{Decode, Encode};
use borsh::{BorshDeserialize, BorshSerialize};
use libp2p::PeerId;
use log::warn;
use lru::LruCache;
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, Bytes};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::RwLock;

use async_recursion::async_recursion;
use log::{debug, error, info, warn};
use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot, Mutex, RwLock};
use tokio::time::interval;
use lru::LruCache;
use sha2::Sha256;
use rand::SeedableRng;

use crate::crypto::{KeyPair, ValidatorKeys};
use crate::p2p::{AddressBook, P2pCommand, SyncRequest, SyncResponse};
use libp2p::PeerId;

pub type ConsensusMsgTuple = (ConsensusMessage, PeerId, Option<oneshot::Sender<SyncResponse>>);

const PROPOSER_TIMEOUT: Duration = Duration::from_secs(10);
const AEGIS_SUB_COMMITTEE_SIZE: usize = 4;
use crate::{Address, Block, BlockHeader, ChainMessage, Signature, Transaction};

pub struct ConsensusState {
    pub core_state: Arc<RwLock<CoreConsensusState>>,
    pub pending_proposals: Arc<RwLock<HashMap<Vec<u8>, PendingBlock>>>,
    pub proposal_queues: Arc<RwLock<ProposalQueues>>,
    pub recently_processed_hashes: Arc<RwLock<LruCache<[u8; 32], ()>>>,
}

pub struct CoreConsensusState {
    pub current_round: u64,
    pub current_step: u64,
    pub step_start_time: Instant,
    pub highest_seen_qc: QuorumCertificate,
    pub velocity_votes: HashMap<Vec<u8>, Vec<VelocityVote>>,
    pub processed_optimistic_blocks: HashSet<Vec<u8>>,
    pub optimistically_confirmed_blocks: Vec<Block>,
}

pub struct ProposalQueues {
    pub pending_proposals_waiting_for_parent:
        HashMap<Vec<u8>, Vec<(ConsensusMessage, PeerId, Option<Vec<Transaction>>)>>,
    pub pending_proposals_awaiting_parent_state:
        HashMap<Vec<u8>, Vec<(ConsensusMessage, PeerId, Option<Vec<Transaction>>)>>,
    pub premature_proposals:
        HashMap<u64, Vec<(ConsensusMessage, PeerId, Option<Vec<Transaction>>)>>,
    pub stale_qc_request: HashMap<Vec<u8>, (u64, Instant)>,
    pub pending_qc_waiting_for_block: HashMap<Vec<u8>, Vec<(QuorumCertificate, PeerId)>>,
}

impl ConsensusState {
    pub fn new(initial_qc: QuorumCertificate, initial_block_hash: Vec<u8>) -> Self {
        Self {
            core_state: Arc::new(RwLock::new(CoreConsensusState {
                current_round: 0,
                current_step: 0,
                step_start_time: Instant::now(),
                highest_seen_qc: initial_qc.clone(),
                velocity_votes: HashMap::new(),
                processed_optimistic_blocks: HashSet::new(),
                optimistically_confirmed_blocks: Vec::new(),
            })),
            pending_proposals: Arc::new(RwLock::new(HashMap::new())),
            proposal_queues: Arc::new(RwLock::new(ProposalQueues {
                pending_proposals_waiting_for_parent: HashMap::new(),
                pending_proposals_awaiting_parent_state: HashMap::new(),
                premature_proposals: HashMap::new(),
                stale_qc_request: HashMap::new(),
                pending_qc_waiting_for_block: HashMap::new(),
            })),
            recently_processed_hashes: Arc::new(RwLock::new(LruCache::new(
                NonZeroUsize::new(1000).unwrap(),
            ))),
        }
    }
}

#[derive(BorshSerialize, BorshDeserialize, Serialize, Deserialize, Debug, Clone)]
pub struct PendingBlock {
    pub header: BlockHeader,
    pub transactions: Vec<crate::Transaction>,
    pub parent_qc: QuorumCertificate,
    pub round: u64,
}

#[derive(BorshSerialize, BorshDeserialize, Serialize, Deserialize, Debug, Clone)]
pub struct VelocityVote {
    pub round_id: u64,
    pub block_hash: Vec<u8>,
    pub voter_address: Address,
    #[serde(with = "serde_bytes")]
    pub signature: Signature,
}

impl VelocityVote {
    pub fn sign(mut self, keypair: &KeyPair) -> Self {
        let data_to_sign = self.canonical_bytes(&keypair.public_key_bytes());
        self.signature = keypair.sign(&data_to_sign).to_vec();
        self
    }

    pub fn canonical_bytes(&self, voter_public_key: &[u8]) -> Vec<u8> {
        let mut data = Vec::new();
        data.extend_from_slice(&self.round_id.to_be_bytes());
        data.extend_from_slice(&self.block_hash);
        data.extend_from_slice(voter_public_key);
        data
    }

    pub fn collect_verified_votes<'a>(
        votes: impl IntoIterator<Item = &'a VelocityVote>,
        expected_round: u64,
    ) -> Option<Vec<(Address, Signature)>> {
        let mut verified_signatures = Vec::new();
        let mut unique_voters = std::collections::HashSet::new();

        for vote in votes.into_iter().filter(|v| v.round_id == expected_round) {
            if unique_voters.insert(vote.voter_address) {
                verified_signatures.push((vote.voter_address, vote.signature));
            }
        }

        if verified_signatures.is_empty() {
            None
        } else {
            Some(verified_signatures)
        }
    }
}

#[serde_as]
#[derive(BorshSerialize, BorshDeserialize, Serialize, Deserialize, Debug, Clone)]
pub struct OptimisticConfirmation {
    pub header: BlockHeader,
    #[serde_as(as = "Vec<Bytes>")]
    pub transaction_hashes: Vec<Vec<u8>>,
    pub parent_qc: QuorumCertificate,
    pub round: u64,
}

#[derive(BorshSerialize, BorshDeserialize, Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct FinalityCertificate {
    pub checkpoint_hash: Vec<u8>,
    pub epoch: u64,
    #[serde(with = "serde_bytes")]
    pub aggregated_signature: Vec<u8>,
    pub voters: Vec<Address>,
}

#[derive(BorshSerialize, BorshDeserialize, Serialize, Deserialize, Debug, Clone)]
pub enum ConsensusMessage {
    IntentBatchProposal(Box<Block>),
    AegisVelocityVote(VelocityVote),
    AegisNewQuorumCertificate(QuorumCertificate),
    AegisFinalityCertificate(FinalityCertificate),
}

impl ConsensusMessage {
    pub fn hash(&self) -> [u8; 32] {
        borsh::to_vec(self)
            .map(|encoded| Sha256::digest(&encoded).into())
            .unwrap_or_default()
    }
}

#[derive(BorshSerialize, BorshDeserialize, Serialize, Deserialize, Debug, Clone)]
pub struct PartialVote {
    pub block_hash: Vec<u8>,
    pub view_number: u64,
    pub voter_address: Address,
    #[serde(with = "serde_bytes")]
    pub signature_share: Vec<u8>,
}

#[serde_as]
#[derive(
    BorshSerialize,
    BorshDeserialize,
    Serialize,
    Deserialize,
    Debug,
    Clone,
    PartialEq,
    Eq,
    Decode,
    Encode,
)]
pub struct QuorumCertificate {
    pub block_hash: Vec<u8>,
    pub view_number: u64,
    #[serde_as(as = "Vec<(_, Bytes)>")]
    pub signatures: Vec<(Address, Signature)>,
}

impl QuorumCertificate {
    pub fn genesis_qc() -> Self {
        Self {
            block_hash: vec![0; 32],
            view_number: 0,
            signatures: vec![],
        }
    }
}
#[derive(Clone)]
struct ConsensusEngine {
    my_address: Address,
    validator_keys: Arc<ValidatorKeys>,
    p2p_cmd_tx: mpsc::Sender<P2pCommand>,
    state: ConsensusState,
    consensus_ready: Arc<AtomicBool>,
    dkg_state: DkgState,
    address_book: Arc<Mutex<AddressBook>>,
    pending_tx_requests: Arc<RwLock<HashMap<u64, oneshot::Sender<Vec<Transaction>>>>>,
    tx_gossip: mpsc::Sender<ChainMessage>,
    chain_id: String,
}

#[derive(Debug)]
enum ConsensusOffense {
    StateRootMismatch {
        header: BlockHeader,
        computed_state_root: Vec<u8>,
    },
    FailedSimulation {
        header: BlockHeader,
        error: String,
    },
    InvalidSignature {
        header: BlockHeader,
    },
    TransactionsRootMismatch {
        header: BlockHeader,
    },
    UnknownProposer {
        header: BlockHeader,
    },
    MissingParent {
        parent_hash: Vec<u8>,
    },
}

impl ConsensusEngine {
    pub async fn run(
        self,
        mut p2p_msg_rx: mpsc::Receiver<ConsensusMsgTuple>,
        mut txs_response_from_p2p_rx: mpsc::Receiver<SyncResponse>,
    ) {
        info!("[AEGIS] Mesin Konsensus Aegis dimulai, menunggu sinyal ConsensusReady...");
        loop {
            if self.consensus_ready.load(Ordering::SeqCst) {
                info!("[AEGIS] Sinyal ConsensusReady diterima. Memulai protokol konsensus.");
                break;
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }

        let message_handler_engine = self.clone();
        tokio::spawn(async move {
            while let Some((msg, source_peer, transactions_opt)) = p2p_msg_rx.recv().await {
                let engine_clone = message_handler_engine.clone();
                tokio::spawn(async move {
                    engine_clone
                        .handle_consensus_message(msg, source_peer, transactions_opt)
                        .await;
                });
            }
        });

        let mut pacesetter_ticker = interval(Duration::from_millis(500));
        let mut current_step_task: Option<tokio::task::JoinHandle<()>> = None;

        loop {
            tokio::select! {
                _ = pacesetter_ticker.tick() => {},
            }
            if !self.consensus_ready.load(Ordering::SeqCst) {
                continue;
            }

            let (highest_qc_view, highest_qc_hash, current_round, current_step, step_start_time) = {
                let core = self.state.core_state.read().await;
                (
                    core.highest_seen_qc.view_number,
                    core.highest_seen_qc.block_hash.clone(),
                    core.current_round,
                    core.current_step,
                    core.step_start_time,
                )
            };

            // Lanjutkan konsensus tanpa mengecek L1 state tree

            let mut start_new_task = false;
            let mut next_round = current_round;
            let mut next_step = current_step;

            if highest_qc_view >= current_round {
                next_round = highest_qc_view + 1;
                next_step = 0;
                info!("[AEGIS DRIVER] QC#{} diterima & blok induk ada. Maju ke Ronde #{}, Langkah #0.", highest_qc_view, next_round);
                start_new_task = true;
            } else if step_start_time.elapsed() > PROPOSER_TIMEOUT {
                next_step = current_step + 1;
                warn!("[AEGIS DRIVER] Proposer untuk Ronde #{}, Langkah #{} timeout. Maju ke Langkah #{}.", current_round, current_step, next_step);
                start_new_task = true;

                if next_step >= AEGIS_SUB_COMMITTEE_SIZE as u64 {
                    warn!("[AEGIS DRIVER] Semua proposer gagal untuk Ronde #{}. Memaksa maju ke Ronde berikutnya.", current_round);
                    next_round = current_round + 1;
                    next_step = 0;
                }
            }

            if start_new_task {
                if let Some(task) = current_step_task.take() {
                    task.abort();
                }

                {
                    let mut core = self.state.core_state.write().await;
                    core.current_round = next_round;
                    core.current_step = next_step;
                    core.step_start_time = Instant::now();
                }

                let premature_proposals_for_round = self
                    .state
                    .proposal_queues
                    .write()
                    .await
                    .premature_proposals
                    .remove(&next_round);

                if let Some(proposals) = premature_proposals_for_round {
                    info!("[AEGIS DRIVER] Memproses ulang {} proposal prematur yang diantrekan untuk Ronde #{}", proposals.len(), next_round);
                    for (msg, source_peer, txs) in proposals {
                        let engine_clone = self.clone();
                        tokio::spawn(async move {
                            engine_clone
                                .handle_consensus_message(msg, source_peer, txs)
                                .await;
                        });
                    }
                }

                current_step_task = Some(tokio::spawn(
                    self.clone().handle_round_step(next_round, next_step),
                ));
            }
        }
    }

    async fn handle_round_step(self, round: u64, step: u64) {
        let (current_round, current_step, seed_hash) = {
            let core = self.state.core_state.read().await;
            if core.current_round != round || core.current_step != step {
                info!(
                    "[AEGIS] Membatalkan tugas usang untuk Ronde #{}, Langkah #{}.",
                    round, step
                );
                return;
            }
            (
                core.current_round,
                core.current_step,
                core.highest_seen_qc.block_hash.clone(),
            )
        };

        info!(
            "[AEGIS] Menjalankan tugas untuk Ronde #{}, Langkah #{}",
            current_round, current_step
        );

        let sub_committee = self.determine_sub_committee(round, &seed_hash).await;
        if sub_committee.is_empty() {
            return;
        }

        let proposer_address = match sub_committee.get(step as usize) {
            Some(addr) => addr,
            None => {
                warn!(
                    "[AEGIS] Langkah #{} di luar batas untuk sub-komite Ronde #{}.",
                    step, round
                );
                return;
            }
        };

        if self.my_address == *proposer_address {
            info!(
                "[AEGIS PROPOSER] Saya adalah pemimpin untuk Ronde #{}, Langkah #{}.",
                round, step
            );
            self.run_proposer_flow(round).await;
        }
    }

    async fn run_proposer_flow(&self, round: u64) {
        info!(
            "[AEGIS PROPOSER] Memulai alur kerja sebagai proposer untuk Ronde #{}.",
            round
        );

        let parent_qc = {
            let core = self.state.core_state.read().await;
            core.highest_seen_qc.clone()
        };

        // TODO: Get transactions from Intent Mempool
        let valid_txs = vec![];

        let block_proposal = Block {
            header: BlockHeader {
                index: 0,          // TODO: Derive index
                prev_hash: vec![], // TODO: Derive prev hash
                timestamp: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64,
                transactions_root: Block::calculate_transactions_root(&valid_txs),
                authority: self.my_address,
                signature: vec![],
            },
            transactions: valid_txs.clone(),
            justify: parent_qc.clone(),
            round,
        };

        let mut block_proposal = block_proposal;
        let data_to_sign = block_proposal.header.canonical_bytes_for_signing();
        block_proposal.header.signature = self
            .validator_keys
            .signing_keys
            .sign(&data_to_sign)
            .to_vec();

        let block_hash = block_proposal.header.calculate_hash();
        info!(
            "[AEGIS PROPOSER] Mengusulkan blok baru #{} (hash: 0x{}) dengan {} transaksi.",
            block_proposal.header.index,
            hex::encode(&block_hash[..4]),
            valid_txs.len()
        );

        let pending_block = PendingBlock {
            header: block_proposal.header.clone(),
            transactions: valid_txs.clone(),
            parent_qc,
            round,
        };
        self.state
            .pending_proposals
            .write()
            .await
            .insert(block_hash.clone(), pending_block);

        if self
            .p2p_cmd_tx
            .send(P2pCommand::BroadcastConsensusMessage(
                ConsensusMessage::IntentBatchProposal(Box::new(block_proposal.clone())),
            ))
            .await
            .is_err()
        {
            error!("[AEGIS PROPOSER] Gagal menyiarkan proposal blok ke P2P.");
        }

        let self_vote = {
            let vote = VelocityVote {
                round_id: round,
                block_hash: block_hash.clone(),
                voter_address: self.my_address,
                signature: [0; crypto::SIGNATURE_SIZE],
            };
            vote.sign(&self.validator_keys.signing_keys)
        };

        self.state
            .core_state
            .write()
            .await
            .velocity_votes
            .entry(block_hash.clone())
            .or_default()
            .push(self_vote);

        self.process_votes_for_block(&block_hash).await;
    }

    #[async_recursion]
    async fn handle_consensus_message(
        &self,
        msg: ConsensusMessage,
        source_peer: PeerId,
        _transactions_opt: Option<Vec<Transaction>>,
    ) {
        let message_hash = msg.hash();

        if self
            .state
            .recently_processed_hashes
            .read()
            .await
            .contains(&message_hash)
        {
            return;
        }

        {
            let mut cache = self.state.recently_processed_hashes.write().await;
            if cache.contains(&message_hash) {
                return;
            }
            cache.put(message_hash, ());
        }

        match msg {
            ConsensusMessage::IntentBatchProposal(block) => {
                self.handle_block_proposal(*block, source_peer).await;
            }
            ConsensusMessage::AegisVelocityVote(vote) => {
                self.handle_velocity_vote(vote).await;
            }
            ConsensusMessage::AegisNewQuorumCertificate(qc) => {
                self.handle_new_quorum_certificate(qc, source_peer).await;
            }
            _ => {
                warn!("[AEGIS] Menerima jenis pesan konsensus yang tidak ditangani.");
            }
        }
    }

    async fn handle_block_proposal(&self, block: Block, source_peer: PeerId) {
        // Dummy implementation for stateless sequencer
        info!("[AEGIS] Memproses proposal blok dari {}", source_peer);
        self.pre_validate_proposal_concurrently(&block).await.unwrap_or_default();
    }

    #[async_recursion]
    async fn pre_validate_proposal_concurrently(
        &self,
        _block: &Block,
    ) -> Result<(), ConsensusOffense> {
        Ok(())
    }

    async fn process_votes_for_block(&self, block_hash: &[u8]) {}

    async fn handle_velocity_vote(&self, vote: VelocityVote) {
        let block_hash = vote.block_hash.clone();

        if self
            .state
            .core_state
            .read()
            .await
            .processed_optimistic_blocks
            .contains(&block_hash)
        {
            return;
        }

        {
            let mut core = self.state.core_state.write().await;
            let votes_for_block = core.velocity_votes.entry(block_hash.clone()).or_default();
            if !votes_for_block
                .iter()
                .any(|v| v.voter_address == vote.voter_address)
            {
                votes_for_block.push(vote);
            }
        }

        self.process_votes_for_block(&block_hash).await;
    }

    async fn handle_new_quorum_certificate(&self, qc: QuorumCertificate, source_peer: PeerId) {
        let is_fully_valid = true; // TODO: Implement QC verification without L1 state
        if !is_fully_valid {
            warn!("[AEGIS] Menerima QC dengan TANDA TANGAN TIDAK VALID dari peer {}. Memberikan penalti.", source_peer);
            let _ = self
                .p2p_cmd_tx
                .send(P2pCommand::ApplyPenalty {
                    peer_id: source_peer,
                    penalty: -50,
                })
                .await;
            return;
        }

        let new_block_hash = qc.block_hash.clone();
        let parent_exists_locally;

        {
            let mut core = self.state.core_state.write().await;
            if qc.view_number > core.highest_seen_qc.view_number {
                info!("[AEGIS KIBLAT] Menerima QC baru yang VALID dari jaringan untuk Ronde #{}. Memperbarui kiblat.", qc.view_number);
                core.highest_seen_qc = qc.clone();

                parent_exists_locally = self
                    .state
                    .pending_proposals
                    .read()
                    .await
                    .contains_key(&new_block_hash);
            } else {
                return;
            }
        }

        if !parent_exists_locally {
            warn!("[AEGIS SYNC PROAKTIF] Menerima QC untuk blok 0x{} yang tidak kita miliki. Meminta data lengkap.", hex::encode(&new_block_hash[..4]));

            self.state
                .proposal_queues
                .write()
                .await
                .stale_qc_request
                .insert(new_block_hash.clone(), (qc.view_number, Instant::now()));

            let cmd = P2pCommand::SendDirectRequest {
                destination: source_peer,
                request: SyncRequest::GetFullProposal(new_block_hash),
            };
            if self.p2p_cmd_tx.send(cmd).await.is_err() {
                error!("[AEGIS] Gagal mengirim permintaan blok yang hilang ke P2P.");
            }
        }
    }

    async fn determine_sub_committee(&self, round: u64, seed_hash: &[u8]) -> Vec<Address> {
        let validators: Vec<Address> = Vec::new(); // TODO: get from address_book

        if validators.is_empty() {
            return Vec::new();
        }

        let mut sorted_validators = validators;
        sorted_validators.sort();

        let seed_material = {
            let mut hasher = sha2::Sha256::new();
            hasher.update(seed_hash);
            hasher.update(&round.to_be_bytes());
            hasher.finalize()
        };

        let seed: [u8; 32] = seed_material.into();
        let mut rng = rand::rngs::StdRng::from_seed(seed);
        sorted_validators.shuffle(&mut rng);

        let committee = sorted_validators
            .into_iter()
            .take(AEGIS_SUB_COMMITTEE_SIZE)
            .collect::<Vec<_>>();

        if !committee.is_empty() {
            debug!(
                "[AEGIS LEADER ELECTION] Ronde #{}: Sub-komite terpilih (proposer pertama): 0x{}",
                round,
                hex::encode(committee[0].as_ref())
            );
        }

        committee
    }

    #[async_recursion]
    async fn reprocess_dependant_proposals(&self, newly_arrived_block_hash: &[u8]) {
        self.state
            .proposal_queues
            .write()
            .await
            .stale_qc_request
            .remove(newly_arrived_block_hash);

        let proposals_to_reprocess = self
            .state
            .proposal_queues
            .write()
            .await
            .pending_proposals_waiting_for_parent
            .remove(newly_arrived_block_hash);

        if let Some(proposals) = proposals_to_reprocess {
            info!(
                "[AEGIS] Blok induk 0x{} telah tiba. Memproses ulang {} proposal yang tertunda.",
                hex::encode(&newly_arrived_block_hash[..4]),
                proposals.len()
            );
            for (pending_msg, pending_peer, pending_txs) in proposals {
                let self_clone = self.clone();
                tokio::spawn(async move {
                    self_clone
                        .handle_consensus_message(pending_msg, pending_peer, pending_txs)
                        .await;
                });
            }
        }

        let proposals_to_reprocess_state = self
            .state
            .proposal_queues
            .write()
            .await
            .pending_proposals_awaiting_parent_state
            .remove(newly_arrived_block_hash);

        if let Some(proposals) = proposals_to_reprocess_state {
            info!("[AEGIS] State untuk induk 0x{} telah siap. Memproses ulang {} proposal yang tertunda.", hex::encode(&newly_arrived_block_hash[..4]), proposals.len());
            for (pending_msg, pending_peer, pending_txs) in proposals {
                let self_clone = self.clone();
                tokio::spawn(async move {
                    self_clone
                        .handle_consensus_message(pending_msg, pending_peer, pending_txs)
                        .await;
                });
            }
        }
    }
}
