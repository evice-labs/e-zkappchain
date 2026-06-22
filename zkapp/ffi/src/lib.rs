// Evice Intent-Centric Engine — FFI Library
//
// This crate provides C-compatible bindings for the Evice matching engine,
// OFA auction system, and intent processing pipeline. Designed to be consumed
// by the Logos Core Module (C++ wrapper) via the universal interface pattern.
//
// Architecture:
// Rust Engine → cdylib (this crate) → cbindgen → C header → C++ Module wraps
//
// This follows the same pattern as LEZ's wallet-ffi crate.

use engine::{EngineEvent, OrderBook, Side};
use error::{print_error, EviceFfiError};
use std::ffi::{c_char, CStr, CString};
use std::sync::{Arc, Mutex, OnceLock};

pub mod error;

// Re-export for cbindgen visibility
pub use error::EviceFfiError as FfiError;

#[repr(C)]
pub struct EviceEngineHandle {
    _private: [u8; 0],
}

struct EngineState {
    orderbook: OrderBook,
}

static ENGINE: OnceLock<Arc<Mutex<EngineState>>> = OnceLock::new();

/// Convert a C string pointer to a Rust String.
fn c_str_to_string(ptr: *const c_char, name: &str) -> Result<String, EviceFfiError> {
    if ptr.is_null() {
        print_error(format!("Null pointer for {name}"));
        return Err(EviceFfiError::NullPointer);
    }

    let c_str = unsafe { CStr::from_ptr(ptr) };
    match c_str.to_str() {
        Ok(s) => Ok(s.to_owned()),
        Err(e) => {
            print_error(format!("Invalid UTF-8 in {name}: {e}"));
            Err(EviceFfiError::InvalidUtf8)
        }
    }
}

/// Allocate a CString on the heap and return a raw pointer
/// The caller Must free this with `evice_free_string`
fn to_c_string(s: &str) -> *mut c_char {
    match CString::new(s) {
        Ok(cs) => cs.into_raw(),
        Err(_) => std::ptr::null_mut(),
    }
}

fn get_engine() -> Result<Arc<Mutex<EngineState>>, EviceFfiError> {
    ENGINE
        .get()
        .cloned()
        .ok_or(EviceFfiError::EngineNotInitialized)
}

fn events_to_fills_json(events: &[EngineEvent]) -> Vec<serde_json::Value> {
    events
        .iter()
        .filter_map(|e| match e {
            EngineEvent::TradeExecuted {
                taker_id,
                maker_id,
                price,
                quantity,
                ..
            } => Some(serde_json::json!({
                "maker_order_id": maker_id,
                "taker_order_id": taker_id,
                "price": price,
                "quantity": quantity,
            })),
            _ => None,
        })
        .collect()
}

/// Initialize the Evice matching engine.
///
/// Must be called before any other function. Safe to call multiple times
/// (subsequent calls are no-ops).
///
/// Returns `EviceFfiError::Success` on success.
#[no_mangle]
pub extern "C" fn evice_engine_init() -> EviceFfiError {
    let _ = ENGINE.get_or_init(|| {
        Arc::new(Mutex::new(EngineState {
            orderbook: OrderBook::new(),
        }))
    });
    EviceFfiError::Success
}

/// Destroy the engine and free all resources.
///
/// After calling this, the engine must be re-initialized with `evice_engine_init`.
/// Currently a no-op since we use OnceLock (engine lives for process lifetime).
/// This is acceptable for the Logos module pattern where modules are loaded once
/// and unloaded when the host process exits.
#[no_mangle]
pub extern "C" fn evice_engine_destroy() -> EviceFfiError {
    EviceFfiError::Success
}

/// Place a limit order on the matching engine.
///
/// # Parameters
/// - `order_json`: JSON string with fields:
///   - `order_id` (u64): Unique order identifier
///   - `user_id` (u64): User identifier (for self-trade prevention)
///   - `side` (string): "bid" or "ask"
///   - `price` (u64): Price in atomic units
///   - `quantity` (u64): Quantity in atomic units
///
/// # Returns
/// JSON string with execution report containing fills. Caller must free
/// with `evice_free_string`. Returns null on error.
#[no_mangle]
pub extern "C" fn evice_place_order(order_json: *const c_char) -> *mut c_char {
    let json_str = match c_str_to_string(order_json, "order_json") {
        Ok(s) => s,
        Err(_) => return std::ptr::null_mut(),
    };

    let engine = match get_engine() {
        Ok(e) => e,
        Err(_) => {
            print_error("Engine not initialized");
            return std::ptr::null_mut();
        }
    };

    // Parse the order JSON
    let parsed: serde_json::Value = match serde_json::from_str(&json_str) {
        Ok(v) => v,
        Err(e) => {
            print_error(format!("Invalid JSON: {e}"));
            return std::ptr::null_mut();
        }
    };

    let order_id = parsed["order_id"].as_u64().unwrap_or(0);
    let user_id = parsed["user_id"].as_u64().unwrap_or(0);
    let side = match parsed["side"].as_str().unwrap_or("bid") {
        "ask" | "Ask" | "ASK" => Side::Ask,
        _ => Side::Bid,
    };
    let price = parsed["price"].as_u64().unwrap_or(0);
    let quantity = parsed["quantity"].as_u64().unwrap_or(0);

    if price == 0 || quantity == 0 {
        print_error("Price and quantity must be > 0");
        return std::ptr::null_mut();
    }

    let mut state = engine.lock().unwrap();

    let events = state
        .orderbook
        .place_limit_order(order_id, user_id, side, price, quantity);

    let fills = events_to_fills_json(&events);

    let response = serde_json::json!({
        "success": true,
        "order_id": order_id,
        "fills": fills,
        "fill_count": fills.len(),
    });

    to_c_string(&response.to_string())
}

/// Cancel an existing order.
///
/// # Parameters
/// - `order_id`: The order ID to cancel.
/// - `user_id`: The user ID (must match order owner for authorization).
///
/// # Returns
/// JSON string with cancellation result. Caller must free with `evice_free_string`.
#[no_mangle]
pub extern "C" fn evice_cancel_order(order_id: u64, user_id: u64) -> *mut c_char {
    let engine = match get_engine() {
        Ok(e) => e,
        Err(_) => {
            print_error("Engine not initialized");
            return std::ptr::null_mut();
        }
    };

    let mut state = engine.lock().unwrap();
    let events = state.orderbook.cancel_order(order_id, user_id);

    let cancelled = events
        .iter()
        .any(|e| matches!(e, EngineEvent::OrderCancelled { .. }));

    let response = serde_json::json!({
        "success": cancelled,
        "order_id": order_id,
        "message": if cancelled { "Order cancelled" } else { "Order not found or unauthorized" },
    });

    to_c_string(&response.to_string())
}

/// Get the current orderbook depth.
///
/// # Parameters
/// - `levels`: Number of price levels to return on each side.
///
/// # Returns
/// JSON string with `bids` and `asks` arrays, each containing `{price, quantity}`.
/// Caller must free with `evice_free_string`.
#[no_mangle]
pub extern "C" fn evice_get_depth(levels: u32) -> *mut c_char {
    let engine = match get_engine() {
        Ok(e) => e,
        Err(_) => {
            print_error("Engine not initialized");
            return std::ptr::null_mut();
        }
    };

    let state = engine.lock().unwrap();
    let (asks, bids) = state.orderbook.get_depth(levels as usize);

    let response = serde_json::json!({
        "bids": bids.iter().map(|l| serde_json::json!({"price": l.price, "quantity": l.quantity})).collect::<Vec<_>>(),
        "asks": asks.iter().map(|l| serde_json::json!({"price": l.price, "quantity": l.quantity})).collect::<Vec<_>>(),
    });

    to_c_string(&response.to_string())
}

/// Get engine health and statistics.
///
/// # Returns
/// JSON string with engine status. Caller must free with `evice_free_string`.
#[no_mangle]
pub extern "C" fn evice_get_status() -> *mut c_char {
    let initialized = ENGINE.get().is_some();

    if !initialized {
        let response = serde_json::json!({
            "initialized": false,
            "error": "Engine not initialized"
        });
        return to_c_string(&response.to_string());
    }

    let engine = get_engine().unwrap();
    let state = engine.lock().unwrap();
    let (asks, bids) = state.orderbook.get_depth(1);

    let response = serde_json::json!({
        "initialized": true,
        "best_bid": bids.first().map(|l| l.price),
        "best_ask": asks.first().map(|l| l.price),
        "version": env!("CARGO_PKG_VERSION"),
    });

    to_c_string(&response.to_string())
}

/// Free a string previously returned by an `evice_*` function.
///
/// # Safety
/// The pointer must have been returned by a prior `evice_*` call.
/// Passing any other pointer is undefined behavior.
#[no_mangle]
pub unsafe extern "C" fn evice_free_string(s: *mut c_char) {
    if !s.is_null() {
        unsafe {
            let _ = CString::from_raw(s);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::CString;

    #[test]
    fn test_engine_lifecycle() {
        assert_eq!(evice_engine_init(), EviceFfiError::Success);
        // Second init is a no-op
        assert_eq!(evice_engine_init(), EviceFfiError::Success);
    }

    #[test]
    fn test_place_order_via_ffi() {
        evice_engine_init();

        let order = CString::new(
            r#"{
            "order_id": 1,
            "user_id": 100,
            "side": "bid",
            "price": 5000,
            "quantity": 10
        }"#,
        )
        .unwrap();

        let result = evice_place_order(order.as_ptr());
        assert!(!result.is_null());

        let result_str = unsafe { CStr::from_ptr(result) }.to_str().unwrap();
        let parsed: serde_json::Value = serde_json::from_str(result_str).unwrap();
        assert_eq!(parsed["success"], true);
        assert_eq!(parsed["order_id"], 1);

        unsafe { evice_free_string(result) };
    }

    #[test]
    fn test_get_status_via_ffi() {
        evice_engine_init();

        let result = evice_get_status();
        assert!(!result.is_null());

        let result_str = unsafe { CStr::from_ptr(result) }.to_str().unwrap();
        let parsed: serde_json::Value = serde_json::from_str(result_str).unwrap();
        assert_eq!(parsed["initialized"], true);

        unsafe { evice_free_string(result) };
    }

    #[test]
    fn test_null_pointer_safety() {
        evice_engine_init();
        let result = evice_place_order(std::ptr::null());
        assert!(result.is_null());
    }
}
