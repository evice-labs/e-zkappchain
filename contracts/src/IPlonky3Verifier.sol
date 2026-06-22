// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

/**
 * @title IPlonky3Verifier
 * @dev Interface standar untuk memverifikasi ZK-Proof dari ekosistem Evice.
 */
interface IPlonky3Verifier {
    function verifyProof(
        bytes32 oldStateRoot,
        bytes32 newStateRoot,
        bytes calldata proof
    ) external view returns (bool);
}