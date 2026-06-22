// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

import {IPlonky3Verifier} from "./IPlonky3Verifier.sol";
import {ReentrancyGuard} from "openzeppelin-contracts/contracts/utils/ReentrancyGuard.sol";
import {Pausable} from "openzeppelin-contracts/contracts/utils/Pausable.sol";
import {Ownable} from "openzeppelin-contracts/contracts/access/Ownable.sol";

/**
 * @title EviceRollup
 * @dev Kontrak utama penyelesaian (Settlement Layer) untuk Evice Intent Ecosystem.
 * Terlindungi dari Reentrancy dan dilengkapi Circuit Breaker darurat.
 */
contract EviceRollup is ReentrancyGuard, Pausable, Ownable {
    // --- State Variables ---
    address public immutable SEQUENCER; 
    bytes32 public currentStateRoot;
    IPlonky3Verifier public verifier;
    uint256 public currentBatchId;

    enum IntentStatus { NONE, LOCKED, RESOLVED }
    mapping(bytes32 => IntentStatus) public intentRegistry;

    // --- Events ---
    event IntentLocked(bytes32 indexed intentId, address indexed user, uint256 amount);
    event IntentSettled(bytes32 indexed intentId, address indexed solver);
    event StateUpdated(uint256 indexed batchId, bytes32 oldStateRoot, bytes32 newStateRoot);
    event VerifierUpdated(address indexed newVerifier);

    // --- Errors ---
    // Menggunakan Custom Errors alih-alih require("string") untuk menghemat gas.
    error UnauthorizedSequencer();
    error InvalidProof();
    error IntentNotLocked();
    error InvalidAmount();

    // --- Modifiers ---
    // Kita memanggil fungsi internal di dalam modifier. 
    // Jika sebuah modifier digunakan berkali-kali, cara ini akan sangat menghemat ukuran kontrak (contract size).
    modifier onlySequencer() {
        _checkSequencer(); 
        _;
    }

    /**
     * @param _initialSequencer Alamat wallet node Sequencer.
     * @param _initialStateRoot Akar Merkle awal.
     * @param _initialOwner Alamat Admin yang bisa menekan tombol Pause.
     */
    constructor(
        address _initialSequencer, 
        bytes32 _initialStateRoot,
        address _initialOwner
    ) Ownable(_initialOwner) {
        SEQUENCER = _initialSequencer;
        currentStateRoot = _initialStateRoot;
        currentBatchId = 0;
    }

    /**
     * @dev Admin dapat menghentikan sistem jika terjadi keadaan darurat
     */
    function pauseSystem() external onlyOwner {
        _pause();
    }

    function unpauseSystem() external onlyOwner {
        _unpause();
    }

    /**
     * @dev User mengunci dana di L1 untuk Intent L2.
     * Ini memberikan jaminan kepada Solver bahwa dana tersedia.
     */
    function depositIntent(bytes32 _intentId) external payable whenNotPaused nonReentrant {
        if (msg.value == 0) revert InvalidAmount();
        
        intentRegistry[_intentId] = IntentStatus.LOCKED;
        emit IntentLocked(_intentId, msg.sender, msg.value);
    }

    /**
     * @dev Memperbarui kontrak Verifier (jika sirkuit Plonky3 kita di-upgrade nanti).
     */
    function setVerifier(address _verifierAddress) external onlySequencer {
        verifier = IPlonky3Verifier(_verifierAddress);
        emit VerifierUpdated(_verifierAddress);
     }

    /**
     * @dev Fungsi utama untuk L2 submit batch ke L1.
     * @param _newStateRoot Akar Merkle baru setelah transaksi L2 dieksekusi.
     * @param _proof Sertifikat kriptografi (ZK-Proof) dari Plonky3.
     */
    function updateStateWithIntents(
        bytes32 _newStateRoot, 
        bytes calldata _proof,
        bytes32[] calldata _resolvedIntentIds
    ) external onlySequencer whenNotPaused nonReentrant {
        bytes32 oldRoot = currentStateRoot;

        // 1. Verifikasi ZK-Proof
        // Sirkuit ZK sekarang harus membuktikan bahwa _resolvedIntentIds benar-benar 
        // diselesaikan dengan output yang diminta user.
        if (!verifier.verifyProof(oldRoot, _newStateRoot, _proof)) revert InvalidProof();

        // 2. Tandai Intent sebagai RESOLVED dan rilis dana (logika sederhana)
        for (uint256 i = 0; i < _resolvedIntentIds.length; i++) {
            bytes32 id = _resolvedIntentIds[i];
            if (intentRegistry[id] == IntentStatus.LOCKED) {
                intentRegistry[id] = IntentStatus.RESOLVED;
                // TODO di Tahap Hardening berikutnya: 
                // Logika mentransfer ETH/ERC20 ke alamat Solver yang terverifikasi.
                emit IntentSettled(id, SEQUENCER); 
            }
        }

        currentStateRoot = _newStateRoot;
        unchecked { currentBatchId++; }

        emit StateUpdated(currentBatchId, oldRoot, _newStateRoot);
    }

    // --- Internal Functions ---
    
    /**
     * @dev Logika validasi untuk modifier onlySequencer.
     */
    function _checkSequencer() internal view {
        if (msg.sender != SEQUENCER) revert UnauthorizedSequencer();
    }
}