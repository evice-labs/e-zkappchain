/**
 * Evice Intent-Centric Engine — FFI Bindings
 *
 * C-compatible interface for the Evice matching engine, OFA auction system,
 * and intent processing pipeline.
 *
 * Thread Safety: All functions are thread-safe. The engine handle uses
 * internal locking to ensure safe concurrent access.
 *
 * Memory Management:
 * - Functions returning `char*` allocate memory that MUST be freed
 *   with `evice_free_string()`
 * - Never free memory returned by FFI using standard C `free()`
 *
 * Initialization:
 * 1. Call `evice_engine_create()` to create an engine instance
 * 2. Use trading/intent functions to interact with the engine
 * 3. Call `evice_engine_destroy()` when done
 *
 * All JSON parameters and return values use UTF-8 encoded strings.
 */


#ifndef EVICE_FFI_H
#define EVICE_FFI_H

/* Generated with cbindgen:0.29.4 */

#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

typedef enum EviceFfiError {
  SUCCESS = 0,
  NULL_POINTER = 1,
  INVALID_UTF8 = 2,
  ENGINE_NOT_INITIALIZED = 3,
  INVALID_JSON = 4,
  ORDER_NOT_FOUND = 5,
  INVALID_ORDER_PARAMS = 6,
  AUCTION_NOT_FOUND = 7,
  INVALID_INTENT = 8,
  BID_REJECTED = 9,
  WAL_ERROR = 10,
  INTERNAL_ERROR = 99,
} EviceFfiError;

typedef struct EviceEngineHandle {
  uint8_t _private[0];
} EviceEngineHandle;

/**
 * Initialize the Evice matching engine.
 *
 * Must be called before any other function. Safe to call multiple times
 * (subsequent calls are no-ops).
 *
 * Returns `EviceFfiError::Success` on success.
 */
enum EviceFfiError evice_engine_init(void);

/**
 * Destroy the engine and free all resources.
 *
 * After calling this, the engine must be re-initialized with `evice_engine_init`.
 * Currently a no-op since we use OnceLock (engine lives for process lifetime).
 * This is acceptable for the Logos module pattern where modules are loaded once
 * and unloaded when the host process exits.
 */
enum EviceFfiError evice_engine_destroy(void);

/**
 * Place a limit order on the matching engine.
 *
 * # Parameters
 * - `order_json`: JSON string with fields:
 *   - `order_id` (u64): Unique order identifier
 *   - `user_id` (u64): User identifier (for self-trade prevention)
 *   - `side` (string): "bid" or "ask"
 *   - `price` (u64): Price in atomic units
 *   - `quantity` (u64): Quantity in atomic units
 *
 * # Returns
 * JSON string with execution report containing fills. Caller must free
 * with `evice_free_string`. Returns null on error.
 */
char *evice_place_order(const char *order_json);

/**
 * Cancel an existing order.
 *
 * # Parameters
 * - `order_id`: The order ID to cancel.
 * - `user_id`: The user ID (must match order owner for authorization).
 *
 * # Returns
 * JSON string with cancellation result. Caller must free with `evice_free_string`.
 */
char *evice_cancel_order(uint64_t order_id, uint64_t user_id);

/**
 * Get the current orderbook depth.
 *
 * # Parameters
 * - `levels`: Number of price levels to return on each side.
 *
 * # Returns
 * JSON string with `bids` and `asks` arrays, each containing `{price, quantity}`.
 * Caller must free with `evice_free_string`.
 */
char *evice_get_depth(uint32_t levels);

/**
 * Get engine health and statistics.
 *
 * # Returns
 * JSON string with engine status. Caller must free with `evice_free_string`.
 */
char *evice_get_status(void);

/**
 * Free a string previously returned by an `evice_*` function.
 *
 * # Safety
 * The pointer must have been returned by a prior `evice_*` call.
 * Passing any other pointer is undefined behavior.
 */
void evice_free_string(char *s);

#endif  /* EVICE_FFI_H */
