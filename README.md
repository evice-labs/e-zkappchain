# Evice ZK-AppChain (e-zkappchain)

Welcome to the **e-zkappchain** repository, the foundational infrastructure for deploying decentralized, high-performance applications (ZK-AppChains) within the **Logos Ecosystem**.

This repository implements a modular architecture designed to separate the decentralized networking layer from the application-specific business logic. While it currently serves as the backbone for a highly optimized, intent-based Decentralized Exchange (DEX), its ultimate vision is to act as a **Multi-Chain Settlement & Liquidity Hub** across the Logos network.

## Modular Architecture

The repository is divided into two primary domains:

1. **`evice-multi-sequencer`**: A generic, application-agnostic decentralized sequencing framework.
2. **`zkapp/*`**: The application layer containing the ZK-AppChain specific logic (DEX, Prover, Engine).

### The `evice-multi-sequencer` Framework

`evice-multi-sequencer` is the beating heart of the decentralized network. It provides a robust, stateless P2P gossip network and consensus engine (Aegis Consensus) that guarantees message ordering and data availability.

**Key Features:**
*   **Application Agnostic (`AppPayload`)**: The sequencer operates purely on raw bytes (`Vec<u8>`). It does not know if it is ordering financial transactions, chess moves, or social media posts.
*   **Stateless by Default**: The consensus engine does not maintain the L1 state tree or evaluate smart contracts, making it incredibly lightweight and lightning-fast.
*   **Plug-and-Play**: Any rust application can import `evice-multi-sequencer`, spawn the `ConsensusEngine`, and listen to the generic `ChainMessage::NewPayload` stream to build their own decentralized peer-to-peer node.

### The Application Layer (`zkapp/*`)

The `zkapp/` directory houses the concrete implementation of our Decentralized Exchange (DEX).

*   **`zkapp/node`**: The binary executable that connects the `evice-multi-sequencer` P2P network to the DEX Matching Engine via an asynchronous "Consensus Bridge".
*   **`zkapp/engine`**: The highly optimized, in-memory Orderbook and Matching Engine.
*   **`zkapp/common`**: Shared cryptographic types and data structures (e.g., the DEX `Transaction`, `BundleTx`).

## The DEX Matching Engine & OFA

The `zkapp/engine` module implements a sophisticated Orderbook and Order Flow Auction (OFA) mechanism.

**How it works:**
1.  **Intent Reception**: Users submit trading intents (e.g., "I want to swap 10 ETH for at least 30,000 USDC") rather than explicit transactions.
2.  **Solver Bidding**: External entities called Solvers monitor these intents and bid to fulfill them. The OFA mechanism ensures users receive the best possible execution price by pitting solvers against each other.
3.  **Matching Engine Execution**: Once an auction concludes, the engine executes the winning bundle. The matching engine is built as an asynchronous Actor model using an in-memory B-Tree structure for sub-millisecond order matching.
4.  **Write-Ahead Logging (WAL)**: To ensure fault tolerance despite running in-memory, every place or cancel command is synchronously written to a WAL (`velocity.wal`) before execution. In case of a node crash, the engine perfectly reconstructs its state by replaying the WAL.
5.  **State Settlement**: The final executed trades and state roots are batched and proven via ZK-Proofs before being settled on the Ethereum L1 via the `SettlementEngine`.

> **Note on `solver-service`:**
> The `zkapp/solver-service` directory is intentionally excluded from this public repository (added to `.gitignore`). It contains proprietary trading algorithms, MEV-extraction strategies, and the core business logic used by our internal solvers to consistently win auctions. This is kept private to protect the competitive advantage of the Evice Market Maker infrastructure.

## Development & Usage

### Prerequisites
- Rust 1.70+
- Protobuf Compiler (`protoc`)

### Building the Project
```bash
cargo build --workspace
```

### Running a Local Node
To run the sequencer and the DEX engine locally in development mode:
```bash
cargo run -p zkapp-node -- --dev --db-path ./node1_data --p2p-port 50000
```
This will initialize a new node, generate local P2P keypairs, and start the gRPC Trading Engine and WebSocket market data server on `127.0.0.1`.

## The Logos Ecosystem & Multi-Chain Vision

The `e-zkappchain` is designed to be a premier citizen of the **Logos Execution Zone**. As development progresses, the repository will evolve to support a fully Multi-Chain paradigm:

1.  **Multi-Chain Intent Resolution**: The matching engine (`zkapp/engine`) will accept intents from various networks (e.g., Ethereum, NomOS, other Logos AppChains). The OFA solvers will route and settle these intents atomically, abstracting away the fragmentation of multi-chain liquidity.
2.  **`e-zkappchain-core`**: The current `evice-multi-sequencer` will be packaged as a standalone, deployable consensus framework. Any developer in the Logos ecosystem will be able to spin up their own ZK-AppChain simply by importing this core and defining their own `AppPayload` logic.
3.  **`e-zkappchain-ui`**: A unified frontend application that will interact with the solver-network and the sequencer. It will provide users with a seamless, single-click trading experience across multiple chains, powered by the intent-based backend.

By decoupling the sequencer (Core) from the Matching Engine (App), we have laid the groundwork for an infrastructure that can scale horizontally across the entire Logos multi-chain landscape.
