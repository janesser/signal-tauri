//! Secondary device linking (QR code flow)
//!
//! This module handles linking a secondary device to an existing Signal account.
//! Since presage 743ed833 performs the entire provisioning flow internally —
//! opening the WebSocket, sending the provisioning URL through a channel as soon
//! as it becomes available, performing the `link_device` handshake and storing
//! registration data in the SQLite store — this module wraps that API.

use parking_lot::Mutex;
use presage::libsignal_service::configuration::SignalServers;
use presage::manager::{Manager, Registered, RegistrationData};
use presage_store_sqlite::SqliteStore;
use futures::channel::oneshot;
use tokio::sync::mpsc;
use serde_json;
use url::Url;

use crate::signal::SignalError;
use crate::signal::SignalEvent;

/// Read the account password that presage stored in its SQLite kv store.
///
/// `Manager::link_secondary_device` generates a random account password, uses it
/// to authenticate the device registration and then stores the full registration
/// data as JSON (key `registration`) in the SQLite kv table. That password is not
/// exposed through presage's public API, so signal-tauri reads it from the store
/// directly in order to authenticate against the storage service (used for
/// message history sync).
pub fn read_account_password(db_path: &std::path::Path) -> Option<String> {
    let conn = rusqlite::Connection::open(db_path).ok()?;
    let value: String = conn.query_row(
        "SELECT value FROM kv WHERE key = 'registration'",
        [],
        |row| row.get(0),
    )
    .ok()?;

    serde_json::from_str::<serde_json::Value>(&value)
        .ok()
        .and_then(|v| v["password"].as_str().map(|s| s.to_string()))
}

static CAPTURED_BACKUP_KEY: Mutex<Option<CapturedProvisioningData>> = Mutex::new(None);

#[derive(Debug, Clone)]
pub struct CapturedProvisioningData {
    pub ephemeral_backup_key: Option<Vec<u8>>,
    pub master_key: Option<Vec<u8>>,
    pub media_root_backup_key: Option<Vec<u8>>,
}

impl CapturedProvisioningData {
    pub fn new() -> Self {
        Self {
            ephemeral_backup_key: None,
            master_key: None,
            media_root_backup_key: None,
        }
    }
}

pub fn store_captured_data(data: CapturedProvisioningData) {
    let mut guard = CAPTURED_BACKUP_KEY.lock();
    *guard = Some(data);
}

pub fn take_captured_data() -> Option<CapturedProvisioningData> {
    let mut guard = CAPTURED_BACKUP_KEY.lock();
    guard.take()
}

/// Whether presage offered to transfer message history (requires the primary
/// device to have provided an ephemeral backup key).
pub fn has_backup_key(data: &RegistrationData) -> bool {
    data.ephemeral_backup_key().is_some()
}

/// Read the ephemeral backup key that presage stored in its SQLite kv store.
///
/// `Manager::link_secondary_device` stores the full registration data as JSON (key `registration`)
/// in the SQLite kv table, and presage keeps the ephemeral backup key that the primary offered
/// while linking inside it. Reading it here lets a restarted app fetch a transfer archive for the
/// same device instead of only having one shot while the link is fresh. Returns `None` if the key
/// was never persisted (an app started through `perform_linking` still carries it in memory).
pub fn read_ephemeral_backup_key(db_path: &std::path::Path) -> Option<[u8; 32]> {
    let conn = rusqlite::Connection::open(db_path).ok()?;
    let value: String = conn.query_row(
        "SELECT value FROM kv WHERE key = 'registration'",
        [],
        |row| row.get(0),
    )
    .ok()?;

    serde_json::from_str::<serde_json::Value>(&value)
        .ok()
        .and_then(|v| v["ephemeral_backup_key"].as_array())
        .map(|k| {
            k.iter()
                .map(|i| i.as_u64().unwrap_or(0) as u8)
                .collect::<Vec<u8>>()
        })
        .filter(|k| k.len() == 32)
        .map(|k| {
            let mut bytes = [0u8; 32];
            bytes.copy_from_slice(&k);
            bytes
        })
}

/// The ephemeral backup key that presage received from the primary device.
///
/// Returns `None` if no history could be transferred.
pub fn get_ephemeral_backup_key(data: &RegistrationData) -> Option<[u8; 32]> {
    data.ephemeral_backup_key()
}

/// Link a secondary device using presage's built-in linking flow.
///
/// This replaces the previous custom WebSocket implementation with presage's own
/// provisioning implementation (maintained upstream by the Signal community and
/// the whisperfish fork). It opens the provisioning WebSocket, sends the URL
/// through a channel as soon as it becomes available so that the QR code can be
/// displayed before the phone scans it, then performs the full `link_device`
/// handshake and registration on behalf of the caller.
pub async fn link_device(
    store: SqliteStore,
    device_name: String,
    event_tx: mpsc::UnboundedSender<SignalEvent>,
) -> Result<(RegistrationData, Manager<SqliteStore, Registered>), SignalError> {
    tracing::info!("Starting secondary device linking with presage...");

    let (url_tx, url_rx) = oneshot::channel::<Url>();

    // Forward the provisioning URL to signal-tauri's event channel so that the
    // QR code can be displayed before the primary device scans it.
    let url_forwarder = tokio::task::spawn(async move {
        match url_rx.await {
            Ok(url) => {
                tracing::info!("Provisioning URL ready: {}", url);
                let _ = event_tx.send(SignalEvent::ProvisioningUrlReady(url.to_string()));
            }
            Err(_) => {
                tracing::error!("Failed to receive provisioning URL");
                let _ = event_tx.send(SignalEvent::LinkingFailed("Failed to receive provisioning URL".to_string()));
            }
        }
    });

    // Use presage's built-in linking. It handles the WebSocket, sends the URL
    // through url_tx and performs the link_device handshake + registration.
    let manager = Manager::link_secondary_device(
        store,
        SignalServers::Production,
        device_name,
        url_tx,
    )
    .await
    .map_err(|e| SignalError::LinkingFailed(e.to_string()))?;

    // Wait for the URL to be forwarded before continuing (the registration
    // happens after it, so this keeps ordering sane).
    let _ = url_forwarder.await;

    tracing::info!(
        "Device linked successfully! ACI: {}, Device ID: {:?}",
        manager.registration_data().service_ids.aci,
        manager.registration_data().device_id
    );

    Ok((manager.registration_data().clone(), manager))
}

/// Full provisioning data received from the primary device during linking.
///
/// Kept as a compatibility structure; presage 743ed833 returns registration
/// data through `RegistrationData` instead, but this remains useful for code
/// that needs to inspect the full message payload.
#[derive(Debug, Clone)]
pub struct FullProvisionMessage {
    pub phone_number: String,
    pub aci: Option<String>,
    pub pni: Option<String>,
    pub provisioning_code: String,
    pub aci_identity_key_public: Vec<u8>,
    pub aci_identity_key_private: Vec<u8>,
    pub pni_identity_key_public: Vec<u8>,
    pub pni_identity_key_private: Vec<u8>,
    pub profile_key: Vec<u8>,
    pub ephemeral_backup_key: Option<Vec<u8>>,
    pub master_key: Option<Vec<u8>>,
    pub media_root_backup_key: Option<Vec<u8>>,
}

/// Run the old provisioning flow (unsupported now — kept for reference only).
#[allow(dead_code)]
pub async fn run_provisioning_capture<F>(
    signal_servers: SignalServers,
    on_url: F,
) -> Result<FullProvisionMessage, SignalError>
where
    F: FnOnce(Url),
{
    Err(SignalError::ProtocolError(
        "run_provisioning_capture is no longer supported; use link_device".to_string(),
    ))
}



#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_captured_data_storage() {
        let _ = take_captured_data();

        let data = CapturedProvisioningData {
            ephemeral_backup_key: Some(vec![1, 2, 3, 4]),
            master_key: Some(vec![5, 6, 7, 8]),
            media_root_backup_key: None,
        };

        store_captured_data(data.clone());

        let retrieved = take_captured_data();
        assert!(retrieved.is_some());
        assert_eq!(
            retrieved.unwrap().ephemeral_backup_key,
            Some(vec![1, 2, 3, 4])
        );
    }

    #[test]
    fn test_provisioning_url_format() {
        let mut url = Url::parse("sgnl://linkdevice").unwrap();
        let test_uuid = "test-uuid-1234";
        let test_pubkey = "dGVzdC1wdWJrZXk=";

        url.query_pairs_mut()
            .append_pair("uuid", test_uuid)
            .append_pair("pub_key", test_pubkey)
            .append_pair("capabilities", "backup4,backup5");

        let url_str = url.to_string();
        assert!(url_str.contains("uuid=test-uuid-1234"));
        assert!(url_str.contains("pub_key="));
        assert!(url_str.contains("capabilities=backup4%2Cbackup5"));
    }
}
