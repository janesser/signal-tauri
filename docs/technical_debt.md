# Technical debt

Known limitations and open work in the app's Signal layer (state after the 743ed833 presage migration + receipt/echo fix, Nov 2026).

## Echo / feedback loop

**Problem.** When we send a message, the Signal server echoes it back (`SyncMessage::Sent`, or — for group messages and some configurations — a `DataMessage` from our own identity) so that all linked devices see it. Previously this echo was treated as an incoming message, so every sent message appeared twice in the conversation.

**Current state.** `handle_incoming_content()` routes server echoes through `handle_our_own_message_echo()`, which looks for a locally-sent message that is still awaiting confirmation (`status = 'sending'`) in the same conversation and at roughly the same timestamp, then confirms it with `update_status(Sent)`. The local store gets its outgoing row from `chat_view::send_message()` (which inserts `MessageStatus::Sending` before handing the message to presage).

- The echo's conversation is derived exactly like `process_sync_message()` does — group messages are identified by their master key first, the destination service ID as a fallback — so an echoed group message resolves to the same conversation row the app would otherwise create.
- A `MessageStatusChanged` event notifies the UI, which marks the status (already up to date) and invalidates the message cache so the "…" → check mark transition renders.

**Remaining risk.** If presage's echo for a *group* message does not carry `group_v2.master_key`, no row is confirmed and the app still creates a duplicate incoming row with the same conversation ID (the old symptom). This can only be verified against a live Signal account; there is no test that exercises it. Also, if two messages to *different* people are sent within the same ±60 s window around the same timestamp, `find_pending()` may confirm the newer one for an echo of the older one — unlikely, and harmless in practice (the status ends up correct either way).

**Confirmed fixes (Nov 17, 2026).** When the server's echo does not report a conversation that matches our local store — e.g. echoes without a destination, or echoes whose `destination_service_id` does not equal the key used by the app's conversations — the old code left the sent message permanently marked `sending` and created a duplicate incoming row in another chat (often appearing as an own-message in the Note-to-Self chat). Echo confirmation now falls back to finding a pending outgoing row across all conversations, matched on send timestamp. Additionally, `SyncMessage::Sent` is only treated as our own echo when the envelope sender is actually us, so messages synced from the primary device are no longer misattributed to ourselves.

## Delivery and read receipts

**Problem.** The app never updated the status of messages it had sent.

**Current state.**
- `ContentBody::ReceiptMessage` (delivery/read/viewed) → `handle_delivery_receipt()`.
- `SyncMessage::Read` over the sync channel (the recipient's other devices) → `handle_sync_read_receipts()`.
- Both match against *all* outgoing messages sent within ±60 s of the receipt timestamp (`find_by_send_timestamp()`), because the server identifies receipts only by the send timestamp of the message and does not tell us which conversation it belongs to — this works for private conversations (recipient = sender of the receipt) and for groups (where a conversation ID is unavailable).
- `mark_delivered()` refuses to downgrade an already-read message, so a late delivery receipt cannot overwrite a read receipt.
- Both paths emit events (`DeliveryReceipt`/`ReadReceipt`) so the UI marks the status in the database (idempotent) and invalidates its message cache for a repaint.

**Remaining risk.** The ±60 s window assumes the app's clock matches the server's within a minute; bigger clock skew silently drops the update. A receipt that arrives *after* the read state has already been applied is skipped by design. Read receipts are only ever marked on messages *we sent* — received messages are never tracked as "read" in this store (which is correct for the Signal protocol: only the sender learns whether a message was read).

## History sync on connect

**Problem.** The app could not retrieve chat history when connecting.

**Current state.** `sync_message_history_from_server()` runs during `perform_linking()`, i.e. immediately after device linking, when the ephemeral backup key (single use, exchanged while linking) is still available. It needs:
- the account password that presage generated while linking, stored in presage's own SQLite kv store (`provisioning::read_account_password`), and
- the auth username `aci.device_id`.

**Remaining risk.** Because the ephemeral backup key is single-use, the sync can only happen *right after* linking. On every later connect the server only delivers new messages through the receive loop — older history is not retrievable again. If presage's kv store no longer holds the account password (e.g. because the store was re-created), the sync fails with "No account password found".

## Missing / stubbed behaviour

Things that exist in the app as placeholders and still need real implementations before the client is complete:

| Area | State |
| --- | --- |
| Sending reactions (`send_reaction`) | Stub — never sent to Signal |
| Sending typing indicators (`send_typing`) | Stub |
| Marking messages read on our side (`mark_read`) | Stub — only logs |
| `MarkRead` over the sync channel (our devices) | Not handled in `handle_incoming_content()` — an incoming `SyncMessage::Read` from *ourselves* would still be treated as a genuine incoming message and could duplicate content |
| UI read-status display | Status icons already render (`chat_view.rs`); they only update once the manager emits receipt events (see "Delivery and read receipts") |
| Voice messages | Only locally recorded; no send/receive path to Signal yet |
| Story, call, and payment messages | Shown as "[Unsupported content]" with a log line |

## Notes on the underlying stack

- The manager's receive loop owns its own presage `SqliteStore` (opened at `storage.signal_db_path()`), while the app's repository layer owns the local SQLite database (`storage.database()`). Both connect to the same Signal store file when the local DB is the Signal store; SQLite connections are locked internally, so concurrent writes are serialized but never merged.
- Timestamps flow through three units: presage gives `u64` millisecond timestamps (receipts, echoes), the app's `sent_at`/`delivered_at` columns store **seconds** since the Unix epoch, and all conversions in this layer divide by 1000 with an explicit `as i64` cast. Never remove those casts — they are a known source of compile errors.
