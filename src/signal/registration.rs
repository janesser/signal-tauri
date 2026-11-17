//! Device registration completion after provisioning
//!
//! NOTE: This module used to implement the final steps of device linking
//! (identity key parsing, pre-key generation, device name encryption and the
//! link_device handshake) directly. Since presage's `Manager::link_secondary_device`
//! performs the entire flow internally on behalf of the caller — including storing
//! registration data in the SQLite store and returning a fully registered manager —
//! this module is kept as a placeholder for any future registration-specific logic.
