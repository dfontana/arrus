# arRPC Application Summary

**arRPC** is a custom Discord Rich Presence daemon that acts as a proxy/bridge between applications and Discord's Rich Presence system. It provides an alternative RPC implementation for applications to connect to Discord without using Discord's official libraries.

## Core Features

1. **Discord Rich Presence Bridge**
   - WebSocket server on port 1337 (configurable via `ARRPC_BRIDGE_PORT`)
   - Forwards activity data to web clients (Discord web app)
   - Maintains last message cache for new connections

2. **Multi-Transport RPC Server**
   - **WebSocket Transport**: Serves Discord web clients on ports 6463-6472
   - **IPC Transport**: Named pipes/Unix sockets for native Discord clients
   - Protocol validation (version 1, JSON encoding only)
   - Client ID and origin validation

3. **Automatic Process Detection**
   - Scans system processes every 5 seconds
   - Matches executables against detectable games database
   - Linux support via /proc filesystem
   - Automatic activity generation for detected games

4. **Database Management**
   - `update_db.js` utility fetches latest game database from Discord API
   - JSON database containing 3000+ game definitions with executable patterns
   - Support for launcher detection and command-line arguments

## Technical Specifications

**Communication Protocols:**
- IPC: Binary framing protocol with handshake, ping/pong, and message types
- WebSocket: JSON-based messaging with Discord RPC command format
- Bridge: Simple JSON broadcast to connected web clients

**Message Types:**
- `SET_ACTIVITY`: Update/clear Rich Presence activity
- `CONNECTIONS_CALLBACK`: Connection status callback
- `GUILD_TEMPLATE_BROWSER`/`INVITE_BROWSER`: Discord invite handling
- `DEEP_LINK`: Deep link parameter handling

**Process Detection:**
- Linux-specific process enumeration via /proc filesystem
- Path normalization and fuzzy matching
- 64-bit identifier removal for better compatibility
- Command-line argument matching for launchers

**Security Features:**
- Origin validation for web connections (Discord domains only)
- Client ID requirements for IPC connections
- Proper socket cleanup and error handling

## Architecture for Rust Implementation

**Core Components:**
1. **Bridge Server** (`bridge.rs`) - WebSocket server for web client communication
2. **RPC Server** (`server.rs`) - Main coordination layer with event handling
3. **Transport Layer** (`transports/`) - WebSocket and IPC protocol implementations
4. **Process Scanner** (`process/`) - Linux process detection
5. **Database Manager** (`db.rs`) - Game database loading and updates

**Key Libraries Needed:**
- `tokio-tungstenite` for WebSocket servers
- `tokio` for async runtime and IPC
- `serde_json` for JSON handling
- Linux-specific APIs for process enumeration (/proc filesystem)
- `reqwest` for database updates

## Implementation Notes

- Target platform: Linux only
- Use async/await patterns throughout
- Implement proper error handling and logging
- Follow Rust best practices for memory safety
- Use structured configuration (environment variables + config files)
- Implement graceful shutdown handling