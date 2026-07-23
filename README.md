# Happy Server

Minimal backend for open-source end-to-end encrypted Claude Code clients.

## What is Happy?

Happy Server is the synchronization backbone for secure Claude Code clients.
It lets multiple devices share supported conversation payloads as encrypted
blobs that the server does not decrypt. The service still stores and processes
account, session, presence, usage, routing, and operational metadata.

## Features

- 🔐 **Encrypted Conversation Content** - Supported client payloads are encrypted before upload
- 🎯 **Minimal Surface** - Only essential features for secure sync, nothing more  
- 🕵️ **Privacy First** - No third-party product analytics or message-content
  mining; operational metrics and required service metadata still exist
- 📖 **Open Source** - Transparent implementation you can audit and self-host
- 🔑 **Cryptographic Auth** - No passwords stored, only public key signatures
- ⚡ **Real-time Sync** - WebSocket-based synchronization across all your devices
- 📱 **Multi-device** - Seamless session management across phones, tablets, and computers
- 🔔 **Push Token Registry** - Authenticated registration, listing, and removal
  of device push tokens; notification delivery is client-owned and best effort
- 🔄 **Reconnect Resilient** - Durable message replay and runtime connection fencing

## How It Works

Your Claude Code clients generate encryption keys locally and use Happy Server as a secure relay. Messages are end-to-end encrypted before leaving your device. The server's job is simple: store encrypted blobs and sync them between your devices in real-time.

The current runtime-owned RPC design supports one Happy server replica. HTTP
reads, durable message storage, cursor replay, and Redis fanout are structured
for broader distribution, but runtime RPC deliberately dispatches to the exact
local socket under a PostgreSQL lease fence. Do not horizontally scale the
server until a receiver-side relay can reacquire that fence before dispatch.

Current operational and protocol constraints are tracked in
[`docs/known-limitations.md`](docs/known-limitations.md).

## Hosting

This fork's production endpoint is `https://server.boujot.com`. Happy Server is
also open source and self-hostable. In either deployment, clients encrypt
supported conversation payloads before transmission; operators must still
protect authentication material, service tokens, metadata, and infrastructure
credentials.

## License

MIT - Use it, modify it, deploy it anywhere.
