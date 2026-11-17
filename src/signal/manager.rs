//! Signal manager - main interface for Signal protocol operations

macro_rules! send_event {
    ($tx:expr, $event:expr) => {{
        let _ = $tx.send($event);
        crate::app::request_repaint();
    }};
}

use crate::signal::messages::MessageStatus;
use crate::signal::provisioning;
use crate::signal::SignalError;
use crate::storage::contacts::{ContactRepository, StoredContact};
use crate::storage::messages::MessageRepository;
use crate::storage::Storage;
use chrono::{DateTime, Utc};
use futures::channel::oneshot;
use futures::StreamExt;
use parking_lot::Mutex;
use presage::libsignal_service::configuration::SignalServers;
use presage::libsignal_service::content::ContentBody;
use presage::libsignal_service::prelude::Content;
use presage::libsignal_service::proto::DataMessage;
use presage::libsignal_service::proto::ReceiptMessage;
use presage::libsignal_service::proto::SyncMessage;
use presage::libsignal_service::proto::receipt_message;
use presage::libsignal_service::proto::sync_message::{self, Content as SyncContent};
use presage::libsignal_service::protocol::ServiceId;
use presage::model::messages::Received;
use presage::manager::{Registered, RegistrationData};
use presage::store::ContentsStore;
use presage::Manager;
use presage_store_sqlite::{OnNewIdentity, SqliteStore};
use rand::distr::{Alphanumeric, SampleString};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;
use uuid::Uuid;

pub enum SendCommand {
    DirectMessage {
        recipient: Uuid,
        text: String,
        reply: oneshot::Sender<Result<(), SignalError>>,
    },
    GroupMessage {
        group_key: Vec<u8>,
        text: String,
        reply: oneshot::Sender<Result<(), SignalError>>,
    },
}

static SEND_TX: Mutex<Option<mpsc::UnboundedSender<SendCommand>>> = Mutex::new(None);

/// Events emitted by the Signal manager
#[derive(Debug, Clone)]
pub enum SignalEvent {
    /// Connection state changed
    ConnectionStateChanged(ConnectionState),
    /// Provisioning URL ready for QR code
    ProvisioningUrlReady(String),
    /// Device linking completed
    LinkingCompleted,
    /// Device linking failed
    LinkingFailed(String),
    /// Message history transfer available from primary device
    MessageHistoryAvailable,
    /// Message history sync progress
    MessageHistorySyncProgress { current: u32, total: u32 },
    /// Message history sync completed
    MessageHistorySyncCompleted { message_count: u32 },
    /// Message history sync failed
    MessageHistorySyncFailed(String),
    /// New message received
    MessageReceived(IncomingMessage),
    /// Message sent successfully
    MessageSent { message_id: String },
    /// Message delivery receipt
    DeliveryReceipt { message_id: String, recipient: String },
    /// Message read receipt
    ReadReceipt { message_id: String, recipient: String },
    /// The status of a message we sent changed
    MessageStatusChanged { message_id: String, status: crate::signal::messages::MessageStatus },
    /// Typing indicator
    TypingStarted { conversation_id: String },
    /// Typing stopped
    TypingStopped { conversation_id: String },
    /// Contact updated
    ContactUpdated { contact_id: String },
    /// Group updated
    GroupUpdated { group_id: String },
    /// Sync completed
    SyncCompleted,
    /// Error occurred
    Error(String),
}

/// Connection state
#[derive(Debug, Clone, PartialEq)]
pub enum ConnectionState {
    Disconnected,
    Connecting,
    Connected,
    Reconnecting,
}

/// Incoming message
#[derive(Debug, Clone)]
pub struct IncomingMessage {
    pub id: String,
    pub sender: String,
    pub conversation_id: String,
    pub content: MessageContent,
    pub timestamp: i64,
    pub server_timestamp: i64,
}

/// Message content types
#[derive(Debug, Clone)]
pub enum MessageContent {
    Text(String),
    Attachment {
        content_type: String,
        filename: Option<String>,
        size: u64,
        attachment_id: String,
    },
    Sticker {
        pack_id: String,
        sticker_id: u32,
    },
    Reaction {
        emoji: String,
        target_message_id: String,
        remove: bool,
    },
    Quote {
        quoted_message_id: String,
        text: String,
    },
}

/// Result of device linking
pub struct LinkingResult {
    pub manager: SignalManager,
}

/// Signal manager for protocol operations
pub struct SignalManager {
    /// Storage reference
    storage: Arc<Storage>,
    /// Event sender
    event_tx: mpsc::UnboundedSender<SignalEvent>,
    /// Event receiver (taken by app)
    event_rx: Option<mpsc::UnboundedReceiver<SignalEvent>>,
    /// Connection state
    connection_state: ConnectionState,
    /// Account phone number (E.164 format)
    phone_number: Option<String>,
    /// Device ID
    device_id: Option<u32>,
}

impl SignalManager {
    /// Start the device linking process
    ///
    /// This spawns a background task that:
    /// 1. Creates the presage store
    /// 2. Initiates device linking
    /// 3. Sends the provisioning URL through the event channel
    /// 4. Completes linking when phone scans QR code
    pub fn start_linking(
        storage: Arc<Storage>,
        device_name: String,
        event_tx: mpsc::UnboundedSender<SignalEvent>,
    ) {
        // Use a dedicated thread for presage operations since its futures aren't Send-safe
        std::thread::spawn(move || {
            // Create a new single-threaded runtime for presage
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("Failed to create runtime for linking");

            rt.block_on(async move {
                match Self::perform_linking(&storage, &device_name, event_tx.clone()).await {
                    Ok(()) => {
                        tracing::info!("Device linking completed successfully");
                        send_event!(event_tx, SignalEvent::LinkingCompleted);
                    }
                    Err(e) => {
                        tracing::error!("Device linking failed: {}", e);
                        send_event!(event_tx, SignalEvent::LinkingFailed(e.to_string()));
                    }
                }
            });
        });
    }

    /// Perform the actual linking process using presage's built-in secondary
    /// device linking.
    ///
    /// Presage handles the WebSocket provisioning, sends the URL through a
    /// channel (forwarded as `SignalEvent::ProvisioningUrlReady`), performs the
    /// full link_device handshake and stores registration data in the SQLite
    /// store — all on behalf of the caller.
    async fn perform_linking(
        storage: &Arc<Storage>,
        device_name: &str,
        event_tx: mpsc::UnboundedSender<SignalEvent>,
    ) -> Result<(), SignalError> {
        tracing::info!("Starting device linking process with presage...");

        // Get database path
        let db_path = storage.signal_db_path();
        let db_url = format!("sqlite://{}", db_path.display());

        tracing::info!("Opening Signal store at: {}", db_url);

        // Create the SQLite store with encryption
        let passphrase = storage.get_encryption_key();

        let store = SqliteStore::open_with_passphrase(
            &db_url,
            passphrase.as_deref(),
            OnNewIdentity::Trust,
        )
        .await
        .map_err(|e| SignalError::StorageError(e.to_string()))?;

        tracing::info!("Signal store opened successfully");

        // Use presage's built-in linking. It returns the registration data (phone
        // number, device id, ephemeral backup key) and a registered manager.
        let (registration_data, mut manager) = provisioning::link_device(
            store,
            device_name.to_string(),
            event_tx.clone(),
        )
        .await?;

        tracing::info!(
            "Device linked successfully! ACI: {}, Device ID: {:?}",
            registration_data.service_ids.aci,
            registration_data.device_id
        );

        storage.save_account(
            &registration_data.phone_number.to_string(),
            registration_data.device_id.unwrap_or(0),
        )
        .map_err(|e| SignalError::StorageError(e.to_string()))?;

        if provisioning::has_backup_key(&registration_data) {
            tracing::info!(
                "Ephemeral backup key available - message history sync possible"
            );
            send_event!(event_tx, SignalEvent::MessageHistoryAvailable);

            match Self::sync_message_history_from_server(
                provisioning::get_ephemeral_backup_key(&registration_data),
                &registration_data,
                storage,
                event_tx.clone(),
            )
            .await
            {
                Ok(count) => {
                    tracing::info!("Message history sync completed: {} messages", count);
                    let _ = event_tx.send(SignalEvent::MessageHistorySyncCompleted { 
                        message_count: count 
                    });
                }
                Err(e) => {
                    tracing::error!("Message history sync failed: {}", e);
                    send_event!(event_tx, SignalEvent::MessageHistorySyncFailed(e.to_string()));
                }
            }
        } else {
            tracing::info!("No ephemeral backup key - message history sync not available");
        }

        tracing::info!("Loading registered manager...");
        let _ = Manager::load_registered(manager.store().clone())
            .await
            .map_err(|e| SignalError::StorageError(format!("Failed to load manager: {:?}", e)));

        tracing::info!("Requesting contacts sync from primary device...");
        if let Err(e) = manager.request_contacts().await {
            tracing::error!("Failed to request contacts sync: {:?}", e);
        } else {
            tracing::info!("Contacts sync requested successfully");
        }

        tracing::info!("Device linked successfully!");
        Ok(())
    }

    /// Create a new Signal manager for device linking (legacy interface)
    /// Sync message history from the primary device via Signal's storage service.
    ///
    /// This requests a transfer archive (an encrypted, gzipped protobuf backup of
    /// the primary device's message history) from Signal's servers and imports it
    /// into local storage. The ephemeral backup key that the primary exchanged while
    /// linking is required to decrypt the archive; it is single use only, so this
    /// sync can only happen while the link is fresh (see also the comment on
    /// `SignalEvent::MessageHistoryAvailable`).
    ///
    /// Whether a new transfer archive is issued on each run depends on how the storage
    /// service handles account credentials - the archive is requested again whenever
    /// this runs. If the server answers 204, which means the primary device may still be
    /// uploading, the request is retried with backoff (see also
    /// `backup::fetch_transfer_archive_with_retries`).
    pub async fn sync_message_history_from_server(
        backup_key: Option<[u8; 32]>,
        registration_data: &RegistrationData,
        storage: &Arc<Storage>,
        event_tx: mpsc::UnboundedSender<SignalEvent>,
    ) -> Result<u32, SignalError> {
        // The ephemeral backup key that the primary offered while linking is stored by presage
        // in its own kv store, so a restarted app can still fetch a transfer archive for this
        // device instead of only having one shot while the link is fresh.
        let Some(backup_key) = backup_key.or_else(|| provisioning::read_ephemeral_backup_key(storage.signal_db_path().as_path())) else {
            tracing::warn!("No ephemeral backup key found - message history sync not possible");
            return Err(SignalError::StorageError("No ephemeral backup key found".to_string()));
        };

        // The account password that presage generated while linking is stored in its SQLite kv
        // store; it authenticates against the storage service.
        let Some(password) = provisioning::read_account_password(storage.signal_db_path().as_path()) else {
            tracing::warn!("No account password found - message history sync not possible");
            return Err(SignalError::StorageError("No account password found".to_string()));
        };

        let auth_username = format!(
            "{}.{}",
            registration_data.service_ids.aci,
            registration_data.device_id.unwrap_or(0)
        );
        tracing::debug!("Using auth username: {}", auth_username);

        event_tx.send(SignalEvent::MessageHistorySyncProgress { current: 0, total: 0 }).ok();

        let backup_info = crate::signal::backup::fetch_transfer_archive_with_retries(&auth_username, &password).await?;
        tracing::info!("Transfer archive available on CDN {}", backup_info.cdn);

        let encrypted_backup = crate::signal::backup::download_backup(&backup_info).await?;
        tracing::info!("Downloaded {} bytes of encrypted backup", encrypted_backup.len());

        let decrypted = crate::signal::backup::decrypt_backup(&encrypted_backup, &backup_key, &registration_data.service_ids.aci)?;
        tracing::info!("Decrypted {} bytes of backup data", decrypted.len());

        let backup_data = crate::signal::backup::parse_backup(&decrypted)?;

        let message_count = backup_data.messages.len() as u32;
        let conversation_count = backup_data.conversations.len();
        tracing::info!(
            "Backup sync complete: {} messages, {} conversations",
            message_count,
            conversation_count
        );

        event_tx.send(SignalEvent::MessageHistorySyncProgress { 
            current: message_count / 2, 
            total: message_count 
        }).ok();

        let (convs_imported, msgs_imported) = crate::signal::backup::import_backup_data(
            &backup_data,
            storage,
        )?;

        tracing::info!(
            "Imported to storage: {} conversations, {} messages",
            convs_imported,
            msgs_imported
        );

        event_tx.send(SignalEvent::MessageHistorySyncProgress { 
            current: message_count, 
            total: message_count 
        }).ok();

        Ok(msgs_imported as u32)
    }

    pub async fn link_device(
        storage: &Arc<Storage>,
        device_name: &str,
    ) -> Result<Self, SignalError> {
        tracing::info!("Creating Signal manager for device linking...");

        let (event_tx, event_rx) = mpsc::unbounded_channel();

        // Start linking in background
        Self::start_linking(storage.clone(), device_name.to_string(), event_tx.clone());

        let manager = Self {
            storage: storage.clone(),
            event_tx,
            event_rx: Some(event_rx),
            connection_state: ConnectionState::Disconnected,
            phone_number: None,
            device_id: None,
        };

        Ok(manager)
    }

    /// Create a Signal manager from existing stored credentials
    /// Takes the app's event_tx to send events to the main event loop
    pub async fn from_storage(
        storage: &Arc<Storage>,
        event_tx: mpsc::UnboundedSender<SignalEvent>,
    ) -> Result<Self, SignalError> {
        tracing::info!("Loading Signal manager from storage...");

        // Check if we have valid credentials
        let db_path = storage.signal_db_path();
        if !db_path.exists() {
            return Err(SignalError::NotRegistered);
        }

        let db_url = format!("sqlite://{}", db_path.display());
        let passphrase = storage.get_encryption_key();

        // Try to open existing store
        let store = SqliteStore::open_with_passphrase(
            &db_url,
            passphrase.as_deref(),
            OnNewIdentity::Trust,
        )
        .await
        .map_err(|e| SignalError::StorageError(e.to_string()))?;

        // Try to load existing manager
        let manager = Manager::load_registered(store)
            .await
            .map_err(|e| SignalError::NotRegistered)?;

        let registration_data = manager.registration_data();

        // Old messages only come from a transfer archive, not from the receive loop - sync
        // history whenever there is no local message history to fall back on.
        let has_history = storage.database()
            .map(|db| MessageRepository::new(&db).total_count() > 0)
            .unwrap_or(false);

        if !has_history {
            tracing::info!("No local message history - syncing history from the primary device");
            match Self::sync_message_history_from_server(None, registration_data, storage, event_tx.clone()).await {
                Ok(count) => event_tx.send(SignalEvent::MessageHistorySyncCompleted {
                    message_count: count,
                }).ok(),
                Err(e) => event_tx.send(SignalEvent::MessageHistorySyncFailed(e.to_string())).ok(),
            }
        } else {
            tracing::info!("Local message history present - skipping history sync");
        }

        let manager = Self {
            storage: storage.clone(),
            event_tx,
            event_rx: None,
            connection_state: ConnectionState::Disconnected,
            phone_number: storage.get_phone_number(),
            device_id: storage.get_device_id(),
        };

        Ok(manager)
    }

    /// Get the event receiver
    pub fn take_event_receiver(&mut self) -> Option<mpsc::UnboundedReceiver<SignalEvent>> {
        self.event_rx.take()
    }

    /// Get connection state
    pub fn connection_state(&self) -> ConnectionState {
        self.connection_state.clone()
    }

    /// Check if connected
    pub fn is_connected(&self) -> bool {
        self.connection_state == ConnectionState::Connected
    }

    /// Get phone number
    pub fn phone_number(&self) -> Option<&str> {
        self.phone_number.as_deref()
    }

    /// Get device ID
    pub fn device_id(&self) -> Option<u32> {
        self.device_id
    }

    pub async fn connect(&mut self) -> Result<(), SignalError> {
        tracing::info!("Connecting to Signal servers...");

        self.connection_state = ConnectionState::Connecting;
        self.event_tx
            .send(SignalEvent::ConnectionStateChanged(ConnectionState::Connecting))
            .ok();

        Self::start_receiving(self.storage.clone(), self.event_tx.clone());

        Ok(())
    }

    pub fn start_receiving(
        storage: Arc<Storage>,
        event_tx: mpsc::UnboundedSender<SignalEvent>,
    ) {
        let (send_tx, send_rx) = mpsc::unbounded_channel::<SendCommand>();
        
        {
            let mut guard = SEND_TX.lock();
            *guard = Some(send_tx);
        }
        
        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("Failed to create runtime for receiving");

            rt.block_on(async move {
                if let Err(e) = Self::receive_loop(&storage, event_tx.clone(), send_rx).await {
                    tracing::error!("Message receive loop failed: {}", e);
                    send_event!(event_tx, SignalEvent::Error(e.to_string()));
                    send_event!(event_tx, SignalEvent::ConnectionStateChanged(ConnectionState::Disconnected));
                }
                
                let mut guard = SEND_TX.lock();
                *guard = None;
            });
        });
    }

    async fn receive_loop(
        storage: &Arc<Storage>,
        event_tx: mpsc::UnboundedSender<SignalEvent>,
        mut send_rx: mpsc::UnboundedReceiver<SendCommand>,
    ) -> Result<(), SignalError> {
        let db_path = storage.signal_db_path();
        let db_url = format!("sqlite://{}", db_path.display());
        let passphrase = storage.get_encryption_key();

        tracing::info!("Opening Signal store for receiving: {}", db_url);

        let store = SqliteStore::open_with_passphrase(
            &db_url,
            passphrase.as_deref(),
            OnNewIdentity::Trust,
        )
        .await
        .map_err(|e| SignalError::StorageError(e.to_string()))?;

        let mut manager = Manager::load_registered(store)
            .await
            .map_err(|_| SignalError::NotRegistered)?;

        // Our own account identity (ACI), used to detect messages that the server
        // echoes back for messages we sent ourselves.
        let own_identity = manager.registration_data().service_ids.aci.to_string();
        tracing::info!("Starting message receive stream as {}", own_identity);

        tracing::info!("Starting message receive stream...");
        send_event!(event_tx, SignalEvent::ConnectionStateChanged(ConnectionState::Connected));

        let messages = manager
            .receive_messages()
            .await
            .map_err(|e| SignalError::ConnectionFailed(format!("{:?}", e)))?;

        futures::pin_mut!(messages);

        loop {
            tokio::select! {
                received = messages.next() => {
                    match received {
                        Some(Received::DecryptionError(sender)) => {
                            tracing::warn!("Got a decryption error from {:?}", sender);
                            send_event!(event_tx, SignalEvent::Error("Decryption failed".to_string()));
                            continue;
                        }
                        Some(Received::QueueEmpty) => {
                            tracing::info!("Message queue synchronized");

                            // Sync profile keys and avatars on startup
                            tracing::info!("Checking for profile key updates...");
                            match crate::signal::profiles::sync_contact_profile_keys(manager.store(), storage).await {
                                Ok(count) if count > 0 => {
                                    tracing::info!("Updated {} contacts with profile keys", count);

                                    // Now sync avatars for contacts with profile keys
                                    tracing::info!("Starting avatar sync...");
                                    match crate::signal::profiles::sync_contact_avatars(&mut manager, storage).await {
                                        Ok(avatar_count) => {
                                            tracing::info!("Synced {} avatars", avatar_count);
                                            if avatar_count > 0 {
                                                if let Err(e) = crate::signal::profiles::update_conversations_from_contacts(storage) {
                                                    tracing::warn!("Failed to update conversations after avatar sync: {}", e);
                                                }
                                                send_event!(event_tx, SignalEvent::ContactUpdated { contact_id: "avatars".to_string() });
                                            }
                                        }
                                        Err(e) => {
                                            tracing::warn!("Avatar sync failed: {}", e);
                                        }
                                    }
                                }
                                Ok(_) => {
                                    tracing::debug!("No profile key updates needed");
                                }
                                Err(e) => {
                                    tracing::warn!("Profile key sync failed: {}", e);
                                }
                            }

                            send_event!(event_tx, SignalEvent::SyncCompleted);
                        }
                        Some(Received::Contacts) => {
                            tracing::info!("Contacts sync signal received, syncing to local database...");
                            if let Err(e) = Self::sync_contacts_to_local(manager.store(), storage).await {
                                tracing::error!("Failed to sync contacts to local storage: {}", e);
                            } else {
                                tracing::info!("Contacts synced to local database");
                                send_event!(event_tx, SignalEvent::ContactUpdated { contact_id: "all".to_string() });

                                if let Err(e) = crate::signal::profiles::update_conversations_from_contacts(storage) {
                                    tracing::warn!("Failed to update conversations from contacts: {}", e);
                                }

                                // First, sync profile keys from presage store
                                tracing::info!("Syncing profile keys from presage store...");
                                match crate::signal::profiles::sync_contact_profile_keys(manager.store(), storage).await {
                                    Ok(count) => {
                                        tracing::info!("Profile key sync complete: {} contacts updated", count);
                                    }
                                    Err(e) => {
                                        tracing::warn!("Profile key sync failed: {}", e);
                                    }
                                }

                                // Then sync avatars (now that profile keys are populated)
                                tracing::info!("Starting avatar sync for contacts...");
                                match crate::signal::profiles::sync_contact_avatars(&mut manager, storage).await {
                                    Ok(count) => {
                                        tracing::info!("Avatar sync complete: {} avatars fetched", count);
                                        if count > 0 {
                                            if let Err(e) = crate::signal::profiles::update_conversations_from_contacts(storage) {
                                                tracing::warn!("Failed to update conversations after avatar sync: {}", e);
                                            }
                                            send_event!(event_tx, SignalEvent::ContactUpdated { contact_id: "avatars".to_string() });
                                        }
                                    }
                                    Err(e) => {
                                        tracing::warn!("Avatar sync failed: {}", e);
                                    }
                                }
                            }
                        }
                        Some(Received::Content(content)) => {
                            Self::log_content_verbose(&content);
                            if let Some(incoming) = Self::handle_incoming_content(
                                storage,
                                &own_identity,
                                event_tx.clone(),
                                &content,
                            ).await {
                                tracing::info!("Received message from {}", incoming.sender);
                                send_event!(event_tx, SignalEvent::MessageReceived(incoming));
                            }
                        }
                        None => {
                            tracing::warn!("Message stream ended");
                            break;
                        }
                    }
                }
                cmd = send_rx.recv() => {
                    match cmd {
                        Some(SendCommand::DirectMessage { recipient, text, reply }) => {
                            let result = Self::send_dm_with_manager(&mut manager, recipient, &text).await;
                            let _ = reply.send(result);
                        }
                        Some(SendCommand::GroupMessage { group_key, text, reply }) => {
                            let result = Self::send_group_with_manager(&mut manager, &group_key, &text).await;
                            let _ = reply.send(result);
                        }
                        None => {
                            tracing::info!("Send channel closed");
                            break;
                        }
                    }
                }
            }
        }

        send_event!(event_tx, SignalEvent::ConnectionStateChanged(ConnectionState::Disconnected));

        Ok(())
    }

    async fn sync_contacts_to_local(
        presage_store: &SqliteStore,
        storage: &Arc<Storage>,
    ) -> Result<(), SignalError> {
        let contacts_iter = presage_store
            .contacts()
            .await
            .map_err(|e| SignalError::StorageError(format!("Failed to get contacts from presage: {:?}", e)))?;

        let presage_contacts: Vec<_> = contacts_iter.filter_map(|r| r.ok()).collect();
        tracing::info!("Found {} contacts in presage store", presage_contacts.len());

        let db = storage
            .database()
            .ok_or_else(|| SignalError::StorageError("App database not available".to_string()))?;
        let repo = ContactRepository::new(&db);

        let now = Utc::now().timestamp();

        for presage_contact in presage_contacts {
            let uuid_str = presage_contact.uuid.to_string();
            let phone_str = presage_contact.phone_number.map(|p| p.to_string());

            let existing = repo.get_by_uuid(&uuid_str);
            let (existing_avatar_path, existing_created_at) = match existing {
                Some(c) => (c.avatar_path, c.created_at),
                None => (None, now),
            };

            let stored_contact = StoredContact {
                id: uuid_str.clone(),
                uuid: uuid_str,
                phone_number: phone_str,
                name: presage_contact.name.clone(),
                profile_name: if presage_contact.name.is_empty() {
                    None
                } else {
                    Some(presage_contact.name)
                },
                avatar_path: existing_avatar_path,
                profile_key: if presage_contact.profile_key.is_empty() {
                    None
                } else {
                    Some(presage_contact.profile_key)
                },
                is_blocked: false,
                is_verified: false,
                created_at: existing_created_at,
                updated_at: now,
            };

            if let Err(e) = repo.save(&stored_contact) {
                tracing::warn!("Failed to save contact {}: {}", stored_contact.id, e);
            }
        }

        tracing::info!("Synced {} contacts to local database", repo.count());
        Ok(())
    }
    
    async fn send_dm_with_manager(
        manager: &mut Manager<SqliteStore, Registered>,
        recipient: Uuid,
        text: &str,
    ) -> Result<(), SignalError> {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| SignalError::SendFailed(e.to_string()))?
            .as_millis() as u64;
        
        let data_message = DataMessage {
            body: Some(text.to_string()),
            timestamp: Some(timestamp),
            ..Default::default()
        };
        
        let service_id = ServiceId::Aci(recipient.into());
        
        manager
            .send_message(service_id, data_message, timestamp)
            .await
            .map_err(|e| SignalError::SendFailed(format!("{:?}", e)))?;
        
        Ok(())
    }
    
    async fn send_group_with_manager(
        manager: &mut Manager<SqliteStore, Registered>,
        master_key: &[u8],
        text: &str,
    ) -> Result<(), SignalError> {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| SignalError::SendFailed(e.to_string()))?
            .as_millis() as u64;
        
        let data_message = DataMessage {
            body: Some(text.to_string()),
            timestamp: Some(timestamp),
            ..Default::default()
        };
        
        manager
            .send_message_to_group(master_key, data_message, timestamp)
            .await
            .map_err(|e| SignalError::SendFailed(format!("{:?}", e)))?;
        
        Ok(())
    }

    fn log_content_verbose(content: &Content) {
        use presage::libsignal_service::content::ContentBody;

        let sender = format!("{:?}", content.metadata.sender);
        let client_timestamp = content.metadata.client_timestamp.timestamp();
        let timestamp = client_timestamp;

        match &content.body {
            ContentBody::DataMessage(dm) => {
                tracing::debug!(
                    "[VERBOSE] DataMessage from={} ts={} body={:?} group={:?} attachments={}",
                    sender,
                    client_timestamp,
                    dm.body.as_ref().map(|b| if b.len() > 50 { format!("{}...", &b[..50]) } else { b.clone() }),
                    dm.group_v2.is_some(),
                    dm.attachments.len()
                );
            }
            ContentBody::SynchronizeMessage(sync) => {
                tracing::info!(
                    "[VERBOSE] SyncMessage from={} ts={}",
                    sender,
                    client_timestamp,
                );
                
                // The sync message content is now an enum of the individual
                // sync message variants (Sent, Request, Keys, ...).
                if let Some(sent) = &sync.content.as_ref()
                    .and_then(|c| match c {
                        presage::libsignal_service::proto::sync_message::Content::Sent(s) => Some(s),
                        _ => None,
                    })
                {
                    tracing::info!(
                        "[VERBOSE]   sent: dest={:?} ts={:?} has_message={} has_story={} edit_message={} unidentified_status={}",
                        sent.destination_service_id,
                        sent.timestamp,
                        sent.message.is_some(),
                        sent.story_message.is_some(),
                        sent.edit_message.is_some(),
                        sent.unidentified_status.len(),
                    );
                    if let Some(msg) = &sent.message {
                        tracing::info!(
                            "[VERBOSE]     message: body={:?} attachments={} group={:?}",
                            msg.body.as_ref().map(|b| if b.len() > 30 { format!("{}...", &b[..30]) } else { b.clone() }),
                            msg.attachments.len(),
                            msg.group_v2.is_some(),
                        );
                    }
                }
                
                if let Some(request) = &sync.content.as_ref()
                    .and_then(|c| match c {
                        presage::libsignal_service::proto::sync_message::Content::Request(r) => Some(r),
                        _ => None,
                    })
                {
                    tracing::info!("[VERBOSE]   request: type={:?}", request.r#type);
                }
                
                if let Some(fetch) = &sync.content.as_ref()
                    .and_then(|c| match c {
                        presage::libsignal_service::proto::sync_message::Content::FetchLatest(f) => Some(f),
                        _ => None,
                    })
                {
                    tracing::info!("[VERBOSE]   fetch_latest: type={:?}", fetch.r#type);
                }
            }
            ContentBody::ReceiptMessage(r) => {
                tracing::debug!("[VERBOSE] ReceiptMessage from={} type={:?} timestamps={:?}", sender, r.r#type, r.timestamp);
            }
            ContentBody::TypingMessage(t) => {
                tracing::debug!("[VERBOSE] TypingMessage from={} action={:?}", sender, t.action);
            }
            ContentBody::CallMessage(_) => {
                tracing::debug!("[VERBOSE] CallMessage from={}", sender);
            }
            ContentBody::NullMessage(_) => {
                tracing::debug!("[VERBOSE] NullMessage from={}", sender);
            }
            ContentBody::StoryMessage(_) => {
                tracing::debug!("[VERBOSE] StoryMessage from={}", sender);
            }
            ContentBody::EditMessage(_) => {
                tracing::debug!("[VERBOSE] EditMessage from={}", sender);
            }
            ContentBody::DecryptionErrorMessage(e) => {
                tracing::warn!(
                    "[VERBOSE] DecryptionErrorMessage from={} error={:?}",
                    sender,
                    e
                );
            }
        }
    }

    /// Handle an incoming content envelope.
    ///
    /// This is the single point where a decrypted message from Signal gets turned
    /// into something the app can work with: delivery and read receipts update the
    /// local message status instead of creating a new message row, messages that
    /// the server echoes back for messages we sent ourselves are confirmed in the
    /// local store (so they don't show up as another, incoming, message), and only
    /// genuine incoming messages are turned into an `IncomingMessage`.
    async fn handle_incoming_content(
        storage: &Arc<Storage>,
        own_identity: &str,
        event_tx: mpsc::UnboundedSender<SignalEvent>,
        content: &Content,
    ) -> Option<IncomingMessage> {
        // Delivery and read receipts update the status of messages we sent instead
        // of adding a new message to the conversation.
        if let ContentBody::ReceiptMessage(receipt) = &content.body {
            Self::handle_delivery_receipt(storage, receipt, content, event_tx.clone()).await;
            return None;
        }

        // Read receipts that arrive via the sync channel — these are sent by the
        // recipient's other devices.
        if let ContentBody::SynchronizeMessage(sync_message) = &content.body {
            if !sync_message.read.is_empty() {
                Self::handle_sync_read_receipts(storage, sync_message, event_tx.clone()).await;
            }
        }

        // The server echoes back the messages we sent ourselves so that all linked
        // devices see them. Those are handled as send confirmations and aren't
        // created as a new message row here.
        if Self::handle_our_own_message_echo(storage, own_identity, content, event_tx.clone()).await.is_some() {
            return None;
        }

        Self::process_content(content)
    }

    /// Mark the messages that a delivery/read receipt refers to in local storage.
    ///
    /// The server identifies the message only by the send timestamp of the message it refers to, not
    /// by conversation or message ID, so we match against all our sent messages at that time. If
    /// nothing matches within the normal 60 s window, a wider fallback covers larger clock drift
    /// between the app and the server - see `find_receipt_targets`.
        let Some(db) = storage.database() else {
            tracing::warn!("No database available, cannot mark messages delivered");
            return;
        };
        let repo = MessageRepository::new(&db);

        // The server identifies the receipt only by the send timestamp of the message it
        // refers to and doesn't tell us which conversation it was in (not for groups), so we
        // match all messages we sent at that time.
        let now = Utc::now();
        let is_read = matches!(receipt.r#type(), receipt_message::Type::Read | receipt_message::Type::Viewed);

        for timestamp in &receipt.timestamp {
            let message_ids = find_receipt_targets(&repo, (*timestamp as i64) / 1000);

            if message_ids.is_empty() {
                tracing::debug!("No sent messages found for receipt at {}", timestamp);
                continue;
            }

            tracing::debug!(
                "Marking {} messages as {:?}",
                message_ids.len(),
                if is_read { MessageStatus::Read } else { MessageStatus::Delivered }
            );

            // Emit an event so the app invalidates its caches and refreshes the display.
            if is_read {
                for message_id in &message_ids {
                    if let Err(e) = repo.mark_read_single(message_id, now) {
                        tracing::warn!("Failed to mark message {} as read: {}", message_id, e);
                    } else {
                        let _ = event_tx.send(SignalEvent::ReadReceipt {
                            message_id: message_id.clone(),
                            recipient: Self::normalize_service_id(&content.metadata.sender.service_id_string()),
                        });
                    }
                }
            } else if let Err(e) = repo.mark_delivered(&message_ids, now) {
                tracing::warn!("Failed to mark messages as delivered: {}", e);
            } else {
                for message_id in &message_ids {
                    let _ = event_tx.send(SignalEvent::DeliveryReceipt {
                        message_id: message_id.clone(),
                        recipient: Self::normalize_service_id(&content.metadata.sender.service_id_string()),
                    });
                }
            }
        }
    }

    /// Find the messages that a delivery/read receipt refers to.
    ///
    /// The server identifies the message only by the send timestamp of the message it refers to, not
    /// by conversation or message ID, so we match against all our sent messages at that time. A ±60 s
    /// window covers normal clock skew between the app and the server; if nothing matches within it, a
    /// wider fallback catches larger drift (a receipt never refers to a message sent long ago).
    fn find_receipt_targets(repo: &MessageRepository, timestamp: i64) -> Vec<String> {
        let narrow = repo.find_by_send_timestamp(timestamp - 60, timestamp + 60);

        if !narrow.is_empty() {
            return narrow;
        }

        tracing::debug!(
            "No sent messages found for receipt at {} within 60 s, retrying with a wider window",
            timestamp
        );
        repo.find_by_send_timestamp(timestamp - 3600, timestamp + 3600)
    }

    /// Mark the messages that a read receipt over the sync channel refers to.
    ///
    /// The read receipts of the other person's devices arrive via the sync channel and are
    /// identified by the timestamp of the message they were sent at, just like delivery
    /// receipts: we match them against our own sent messages instead of relying on a
    /// conversation ID that the server doesn't provide.
    async fn handle_sync_read_receipts(
        storage: &Arc<Storage>,
        sync_message: &SyncMessage,
        event_tx: mpsc::UnboundedSender<SignalEvent>,
    ) {
        let Some(db) = storage.database() else { return };
        let repo = MessageRepository::new(&db);

        let now = Utc::now();

        for read in &sync_message.read {
            let Some(timestamp) = read.timestamp else { continue };

            let message_ids = find_receipt_targets(&repo, (timestamp as i64) / 1000);

            if message_ids.is_empty() {
                tracing::debug!("No sent messages found for read receipt at {}", timestamp);
                continue;
            }

            tracing::debug!(
                "Marking {} messages as read",
                message_ids.len()
            );

            for message_id in &message_ids {
                if let Err(e) = repo.mark_read_single(message_id, now) {
                    tracing::warn!("Failed to mark message {} as read: {}", message_id, e);
                } else {
                    let _ = event_tx.send(SignalEvent::ReadReceipt {
                        message_id: message_id.clone(),
                        recipient: read.sender_aci.clone().unwrap_or_default(),
                    });
                }
            }
        }
    }

    /// Confirm a message that the server echoed back for one we sent ourselves.
    ///
    /// When we send a message, the Signal server echoes it back to us as a
    /// `SyncMessage::Sent` (or, in some configurations, as a direct content
    /// message from our own identity) so that all linked devices see it. If this
    /// echo was also saved as an *incoming* message, every sent message would
    /// appear twice in the conversation — the "echo/feedback loop".
    ///
    /// This looks for a locally-sent message that matches this echo. A send that is
    /// still awaiting confirmation gets confirmed (status `sending` → `sent`); an
    /// echo of a send that was already confirmed would create a duplicate row, so it
    /// is suppressed instead. Nothing is treated as our own echo unless the local
    /// store actually knows about the matching send.
    async fn handle_our_own_message_echo(
        storage: &Arc<Storage>,
        own_identity: &str,
        content: &Content,
        event_tx: mpsc::UnboundedSender<SignalEvent>,
    ) -> Option<String> {
        // Only messages that we sent ourselves are candidates here — everything
        // else is a genuine incoming message. Sync messages from our other devices
        // (e.g. the primary) are not echoes of our own sends.
        let is_echo = match &content.body {
            ContentBody::SynchronizeMessage(SyncMessage {
                content: Some(SyncContent::Sent(_)),
                ..
            }) => content.metadata.sender.raw_uuid().to_string() == own_identity,
            ContentBody::DataMessage(_) => content.metadata.sender.raw_uuid().to_string() == own_identity,
            _ => false,
        };

        if !is_echo {
            return None;
        }

        // Determine the conversation this message belongs to. Some echoes don't report one
        // (self-delivered messages), in which case we search across all conversations below.
        let (conversation_id, timestamp) = match &content.body {
            ContentBody::SynchronizeMessage(SyncMessage {
                content: Some(SyncContent::Sent(sent)),
                ..
            }) => {
                // Group messages are identified by the base64 master key of the group - exactly
                // like `process_sync_message` would - falling back to the destination service ID.
                let conversation_id = sent.message.as_ref()
                    .and_then(|msg| msg.group_v2.as_ref())
                    .and_then(|group| group.master_key.as_ref())
                    .map(|master_key| {
                        base64::Engine::encode(
                            &base64::engine::general_purpose::STANDARD,
                            master_key,
                        )
                    })
                    .or_else(|| sent.destination_service_id.as_deref().map(Self::normalize_service_id));

                match conversation_id {
                    Some(id) => (Some(id), sent.timestamp.unwrap_or_default()),
                    None => (None, sent.timestamp.unwrap_or_default()),
                }
            }
            ContentBody::DataMessage(data_msg) => {
                let conversation_id = if let Some(group) = &data_msg.group_v2 {
                    match group.master_key.as_ref() {
                        Some(master_key) => base64::Engine::encode(
                            &base64::engine::general_purpose::STANDARD,
                            master_key,
                        ),
                        None => content.metadata.sender.raw_uuid().to_string(),
                    }
                } else {
                    content.metadata.sender.raw_uuid().to_string()
                };
                (Some(conversation_id), data_msg.timestamp.unwrap_or(content.metadata.client_timestamp.timestamp() as u64))
            }
            _ => return None,
        };

        let Some(db) = storage.database() else {
            tracing::warn!("No database available, cannot confirm message echo");
            return None;
        };
        let repo = MessageRepository::new(&db);

        // Match this echo against our own sends in the local store. The server may not report
        // the conversation reliably (group echoes without group context, or echoes without a
        // destination), so if nothing matches the reported conversation we also look across all
        // conversations by timestamp - this is how we know which of our own messages was echoed
        // back.
        let timestamp = (timestamp as i64) / 1000;

        // A send that is still awaiting confirmation gets confirmed here, so the echo doesn't
        // sit at `sending` while its duplicate shows up as an incoming message. The content has
        // to match too - messages synced from one of our other devices carry the same account
        // identity and could otherwise be swallowed as if they were our own echo.
        if let Some(message_id) = match &conversation_id {
            Some(id) => repo.find_pending(id, timestamp - 60, timestamp + 60),
            None => repo.find_pending_across_all_conversations(timestamp - 60, timestamp + 60, timestamp),
        } {
            let matches = match repo.get(&message_id) {
                Some(message) => Self::content_matches_echo(&message.content, &content.body),
                None => false,
            };

            if !matches {
                tracing::debug!(
                    "Echo at {} doesn't match the pending send {}",
                    timestamp,
                    message_id
                );
                return None;
            }

            if let Err(e) = repo.update_status(&message_id, MessageStatus::Sent) {
                tracing::warn!("Failed to confirm message {}: {}", message_id, e);
                return None;
            }

            // Notify the app so that it refreshes the message status in the UI.
            send_event!(event_tx, SignalEvent::MessageStatusChanged { message_id: message_id.clone(), status: MessageStatus::Sent });

            tracing::debug!("Confirmed message {} echoed back by the server", message_id);

            return Some(message_id);
        }

        // No pending send at this time. An echo of a send that was already confirmed (a late
        // echo, or one that missed its group context) would create a duplicate row if kept, so
        // it is suppressed instead. The reported conversation narrows the search; only when the
        // server didn't report one reliably do we look across all conversations - again by
        // timestamp proximity, so the closest match wins. A message synced from our other
        // device at the same moment has different content and isn't matched here.
        let known_message_id = match &conversation_id {
            Some(id) => repo.find_our_own_sent_message(id, timestamp - 60, timestamp + 60, timestamp).or_else(|| {
                tracing::debug!("No local send found for echo in conversation {} at {}", id, timestamp);
                repo.find_our_own_sent_across_all_conversations(timestamp - 60, timestamp + 60, timestamp)
            }),
            None => repo.find_our_own_sent_across_all_conversations(timestamp - 60, timestamp + 60, timestamp),
        };

        if let Some(message_id) = known_message_id {
            tracing::debug!("Suppressing duplicate echo of already confirmed message {}", message_id);
            return Some(message_id);
        }

        // The store doesn't know about the send this echo refers to - it stays a genuine incoming message.
        None
    }

    /// Check whether an incoming content envelope matches a local outgoing message.
    ///
    /// Messages we send and messages synced from one of our other devices both arrive with
    /// our account identity, so a text body comparison is the only way to tell them apart: a
    /// genuine echo carries the exact same content as the send it confirms.
    fn content_matches_echo(local: &crate::signal::messages::Content, echo_body: &ContentBody) -> bool {
        match (local, echo_body) {
            (
                crate::signal::messages::Content::Text { body, .. },
                ContentBody::DataMessage(msg),
            ) => msg.body.as_deref() == Some(body),
            _ => false,
        }
    }

    fn process_content(content: &Content) -> Option<IncomingMessage> {
        let sender = content.metadata.sender.raw_uuid().to_string();
        let timestamp = content.metadata.client_timestamp.timestamp() as i64;

        match &content.body {
            ContentBody::DataMessage(data_msg) => {
                Self::process_data_message(data_msg, &sender, timestamp)
            }
            ContentBody::SynchronizeMessage(sync_msg) => {
                Self::process_sync_message(sync_msg, &sender, timestamp)
            }
            ContentBody::TypingMessage(typing) => {
                tracing::debug!("Received typing indicator: {:?}", typing);
                None
            }
            _ => {
                tracing::debug!("Received other message type");
                None
            }
        }
    }

    fn process_data_message(
        data_msg: &DataMessage,
        sender: &str,
        timestamp: i64,
    ) -> Option<IncomingMessage> {
        let text = data_msg.body.clone().unwrap_or_default();
        if text.is_empty() && data_msg.attachments.is_empty() {
            return None;
        }

        let conversation_id = if let Some(group) = &data_msg.group_v2 {
            if let Some(master_key) = &group.master_key {
                use base64::Engine;
                base64::engine::general_purpose::STANDARD.encode(master_key)
            } else {
                sender.to_string()
            }
        } else {
            sender.to_string()
        };

        Some(IncomingMessage {
            id: uuid::Uuid::new_v4().to_string(),
            sender: sender.to_string(),
            conversation_id,
            content: MessageContent::Text(text),
            timestamp,
            server_timestamp: timestamp,
        })
    }

    /// Normalize a service ID string to plain UUID format.
    /// Signal uses formats like "ACI:uuid" or "PNI:uuid" - this strips the prefix.
    fn normalize_service_id(service_id: &str) -> String {
        if let Some(uuid_part) = service_id.strip_prefix("ACI:") {
            uuid_part.to_lowercase()
        } else if let Some(uuid_part) = service_id.strip_prefix("PNI:") {
            uuid_part.to_lowercase()
        } else {
            service_id.to_lowercase()
        }
    }

    fn process_sync_message(
        sync_msg: &presage::libsignal_service::proto::SyncMessage,
        _sender: &str,
        timestamp: i64,
    ) -> Option<IncomingMessage> {
        // In presage 743ed833 the sync message fields are now wrapped in a
        // `content: Option<SyncContent>` enum, so the sent-message variant is
        // matched directly from that.
        // `content` is now an enum of sync message variants; only the sent
        // variant carries a real data message that we can show to the user.
        if let Some(
            presage::libsignal_service::proto::sync_message::Content::Sent(sent),
        ) = &sync_msg.content
        {
            if let Some(data_msg) = &sent.message {
                let text = data_msg.body.clone().unwrap_or_default();
                if text.is_empty() && data_msg.attachments.is_empty() {
                    return None;
                }

                let conversation_id = if let Some(group) = &data_msg.group_v2 {
                    if let Some(master_key) = &group.master_key {
                        use base64::Engine;
                        base64::engine::general_purpose::STANDARD.encode(master_key)
                    } else if let Some(dest) = &sent.destination_service_id {
                        Self::normalize_service_id(dest)
                    } else {
                        return None;
                    }
                } else if let Some(dest) = &sent.destination_service_id {
                    Self::normalize_service_id(dest)
                } else {
                    return None;
                };

                let msg_timestamp = sent.timestamp.unwrap_or(timestamp as u64) as i64;

                tracing::info!(
                    "Received sync of sent message to {} at {}",
                    conversation_id,
                    msg_timestamp
                );

                return Some(IncomingMessage {
                    id: uuid::Uuid::new_v4().to_string(),
                    sender: "self".to_string(),
                    conversation_id,
                    content: MessageContent::Text(text),
                    timestamp: msg_timestamp,
                    server_timestamp: timestamp,
                });
            }
        }

        tracing::debug!("Received sync message without sent content");
        None
    }

    /// Disconnect from Signal servers
    pub async fn disconnect(&mut self) -> Result<(), SignalError> {
        tracing::info!("Disconnecting from Signal servers...");

        self.connection_state = ConnectionState::Disconnected;
        self.event_tx
            .send(SignalEvent::ConnectionStateChanged(ConnectionState::Disconnected))
            .ok();

        Ok(())
    }

    pub async fn send_message(
        &self,
        recipient: &str,
        text: &str,
    ) -> Result<String, SignalError> {
        let message_id = Uuid::new_v4().to_string();
        
        let recipient_uuid = Uuid::parse_str(recipient)
            .map_err(|e| SignalError::SendFailed(format!("Invalid recipient UUID: {}", e)))?;
        
        let event_tx = self.event_tx.clone();
        let text = text.to_string();
        let msg_id = message_id.clone();
        
        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("Failed to create runtime for sending");
            
            rt.block_on(async move {
                match Self::send_via_channel(SendCommand::DirectMessage {
                    recipient: recipient_uuid,
                    text,
                    reply: oneshot::channel().0,
                }).await {
                    Ok(()) => {
                        tracing::info!("Message {} sent successfully", msg_id);
                        send_event!(event_tx, SignalEvent::MessageSent { message_id: msg_id });
                    }
                    Err(e) => {
                        tracing::error!("Failed to send message {}: {}", msg_id, e);
                        send_event!(event_tx, SignalEvent::Error(format!("Send failed: {}", e)));
                    }
                }
            });
        });
        
        Ok(message_id)
    }

    pub async fn send_group_message(
        &self,
        group_id: &str,
        text: &str,
    ) -> Result<String, SignalError> {
        let message_id = Uuid::new_v4().to_string();
        
        let master_key = base64::Engine::decode(
            &base64::engine::general_purpose::STANDARD,
            group_id,
        ).map_err(|e| SignalError::SendFailed(format!("Invalid group ID: {}", e)))?;
        
        let event_tx = self.event_tx.clone();
        let text = text.to_string();
        let msg_id = message_id.clone();
        
        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("Failed to create runtime for sending");
            
            rt.block_on(async move {
                match Self::send_via_channel(SendCommand::GroupMessage {
                    group_key: master_key,
                    text,
                    reply: oneshot::channel().0,
                }).await {
                    Ok(()) => {
                        tracing::info!("Group message {} sent successfully", msg_id);
                        send_event!(event_tx, SignalEvent::MessageSent { message_id: msg_id });
                    }
                    Err(e) => {
                        tracing::error!("Failed to send group message {}: {}", msg_id, e);
                        send_event!(event_tx, SignalEvent::Error(format!("Send failed: {}", e)));
                    }
                }
            });
        });
        
        Ok(message_id)
    }

    /// Send a reaction
    pub async fn send_reaction(
        &self,
        _conversation_id: &str,
        message_id: &str,
        emoji: &str,
        remove: bool,
    ) -> Result<(), SignalError> {
        tracing::info!(
            "Sending reaction {} to message {} (remove: {})",
            emoji,
            message_id,
            remove
        );

        Ok(())
    }

    /// Mark messages as read
    pub async fn mark_read(
        &self,
        _conversation_id: &str,
        message_ids: &[String],
    ) -> Result<(), SignalError> {
        tracing::info!("Marking {} messages as read", message_ids.len());
        Ok(())
    }

    /// Send typing indicator
    pub async fn send_typing(&self, _conversation_id: &str, _is_typing: bool) -> Result<(), SignalError> {
        Ok(())
    }

    pub async fn request_sync(&self) -> Result<(), SignalError> {
        tracing::info!("Requesting sync from primary device...");
        Ok(())
    }
    
    pub async fn send_message_static(
        _storage: &Arc<Storage>,
        recipient: &str,
        text: &str,
    ) -> Result<(), SignalError> {
        let recipient_uuid = Uuid::parse_str(recipient)
            .map_err(|e| SignalError::SendFailed(format!("Invalid recipient UUID: {}", e)))?;
        
        Self::send_via_channel(SendCommand::DirectMessage {
            recipient: recipient_uuid,
            text: text.to_string(),
            reply: oneshot::channel().0,
        }).await
    }
    
    pub async fn send_group_message_static(
        _storage: &Arc<Storage>,
        group_id: &str,
        text: &str,
    ) -> Result<(), SignalError> {
        let master_key = base64::Engine::decode(
            &base64::engine::general_purpose::STANDARD,
            group_id,
        ).map_err(|e| SignalError::SendFailed(format!("Invalid group ID: {}", e)))?;
        
        Self::send_via_channel(SendCommand::GroupMessage {
            group_key: master_key,
            text: text.to_string(),
            reply: oneshot::channel().0,
        }).await
    }
    
    async fn send_via_channel(mut cmd: SendCommand) -> Result<(), SignalError> {
        let (tx, rx) = oneshot::channel();
        
        match &mut cmd {
            SendCommand::DirectMessage { reply, .. } => *reply = tx,
            SendCommand::GroupMessage { reply, .. } => *reply = tx,
        }
        
        let send_tx = {
            let guard = SEND_TX.lock();
            guard.clone()
        };
        
        let send_tx = send_tx.ok_or_else(|| {
            SignalError::SendFailed("Not connected - receive loop not running".to_string())
        })?;
        
        send_tx.send(cmd).map_err(|_| {
            SignalError::SendFailed("Send channel closed".to_string())
        })?;
        
        rx.await.map_err(|_| {
            SignalError::SendFailed("Response channel closed".to_string())
        })?
    }
}
