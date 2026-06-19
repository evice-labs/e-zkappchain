// crates/ffi/src/error.rs
//
// Error handling for the Evice FFI layer.
// Uses numeric error codes following the LEZ wallet-ffi pattern.

use std::str::Utf8Error;

#[repr(C)]
#[must_use]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EviceFfiError {
    Success = 0,
    NullPointer = 1,
    InvalidUtf8 = 2,
    EngineNotInitialized = 3,
    InvalidJson = 4,
    OrderNotFound = 5,
    InvalidOrderParams = 6,
    AuctionNotFound = 7,
    InvalidIntent = 8,
    BidRejected = 9,
    WalError = 10,
    InternalError = 99,
}

impl From<Utf8Error> for EviceFfiError {
    fn from(_value: Utf8Error) -> Self {
        Self::InvalidUtf8
    }
}

impl EviceFfiError {
    /// Check if it's [`EviceFfiError::Success`] or panic
    pub fn unwrap(self) {
        let Self::Success = self else {
            panic!("Called `unwrap()` on error value `{self:#?}`");
        };
    }
}

pub fn print_error(msg: impl Into<String>) {
    eprintln!("[evice-ffi] {}", msg.into());
}
