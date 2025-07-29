# Process Detection Implementation Plan

## Overview

The Process Detection component is responsible for automatically detecting running games on the system and generating Rich Presence activities for Discord. This document outlines a comprehensive implementation plan for a Rust-based process detection system targeting Linux, using the `/proc` filesystem for process enumeration.

## Current Implementation Analysis

The existing Node.js implementation (`/home/koss/code/arrus/ref/arrpc/src/process/index.js`) provides:
- Process scanning via `/proc` filesystem
- Path normalization and matching against a game database
- Activity generation and lifecycle management
- 5-second scanning intervals

### Key Components Analyzed

1. **Process Scanning** (`/home/koss/code/arrus/ref/arrpc/src/process/native/linux.js`):
   - Reads `/proc` directory entries
   - Extracts command line from `/proc/{pid}/cmdline`
   - Returns `[pid, executable_path, arguments]` tuples

2. **Game Database** (`/home/koss/code/arrus/ref/arrpc/src/process/detectable.json`):
   - Contains ~3000+ game entries
   - Structure: `{ id, name, executables: [{ name, os, is_launcher, arguments }] }`
   - Supports multiple executables per game
   - Platform-specific matching (`os` field)

3. **Path Processing**:
   - Lowercase normalization
   - Backslash to forward slash conversion
   - 64-bit identifier removal patterns
   - Suffix path generation for matching

## Architecture Design

### Core Components

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   ProcessDetector│    │   GameDatabase  │    │  ActivityManager│
│                 │    │                 │    │                 │
│ - Scanner       │───▶│ - Loader        │───▶│ - Generator     │
│ - PathProcessor │    │ - Matcher       │    │ - Tracker       │
│ - Scheduler     │    │ - Cache         │    │ - Publisher     │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

### Data Flow

1. **Scan Trigger** (every 5 seconds)
2. **Process Enumeration** (`/proc` reading)
3. **Path Normalization** (cleaning and variant generation)
4. **Database Matching** (executable pattern matching)
5. **Activity Generation** (Discord RPC activity creation)
6. **State Management** (tracking active games)
7. **Event Publishing** (sending to RPC server)

## Detailed Implementation Plan

### 1. Process Scanner Module

**File**: `src/process/scanner.rs`

```rust
pub struct ProcessScanner {
    proc_path: PathBuf,
}

pub struct ProcessInfo {
    pub pid: u32,
    pub executable_path: String,
    pub arguments: Vec<String>,
}
```

**Key Functions**:
- `scan_processes() -> Result<Vec<ProcessInfo>, ProcessError>`
- `read_cmdline(pid: u32) -> Result<(String, Vec<String>), io::Error>`
- `parse_cmdline(content: &str) -> (String, Vec<String>)`

**Implementation Details**:
1. **Directory Reading**: Use `std::fs::read_dir("/proc")` to enumerate PIDs
2. **PID Filtering**: Parse directory names as integers, skip non-numeric entries
3. **Cmdline Reading**: Read `/proc/{pid}/cmdline` as UTF-8 bytes
4. **Null-byte Splitting**: Split on `\0` to separate executable and arguments
5. **Error Handling**: Gracefully handle missing/inaccessible processes

**Linux-specific Considerations**:
- Processes may disappear between enumeration and reading
- `/proc/{pid}/cmdline` may be empty for kernel threads
- Handle permission errors for processes owned by other users
- Some processes may have empty command lines

### 2. Path Processing Module

**File**: `src/process/path_processor.rs`

```rust
pub struct PathProcessor;

pub struct ProcessedPath {
    pub original: String,
    pub normalized: String,
    pub variants: Vec<String>,
}
```

**Key Functions**:
- `process_path(path: &str) -> ProcessedPath`
- `normalize_path(path: &str) -> String`
- `generate_variants(path: &str) -> Vec<String>`
- `remove_64bit_identifiers(path: &str) -> String`

**Normalization Algorithm**:
1. **Case Conversion**: Convert to lowercase using `to_lowercase()`
2. **Separator Normalization**: Replace `\` with `/`
3. **Path Segmentation**: Split by `/` and generate suffix combinations
4. **64-bit Removal**: Apply patterns: `64`, `.x64`, `x64`, `_64`

**Example Processing**:
```
Input: "/usr/games/SteamLibrary/steamapps/common/Counter-Strike Global Offensive/csgo_linux64"

Normalized: "/usr/games/steamlibrary/steamapps/common/counter-strike global offensive/csgo_linux64"

Suffix Variants:
- "csgo_linux64"
- "counter-strike global offensive/csgo_linux64"
- "common/counter-strike global offensive/csgo_linux64"
- ...

64-bit Variants:
- "csgo_linux" (removed "64")
- "counter-strike global offensive/csgo_linux"
- ...
```

### 3. Game Database Module

**File**: `src/process/database.rs`

```rust
#[derive(Debug, Clone, Deserialize)]
pub struct GameEntry {
    pub id: String,
    pub name: String,
    pub executables: Vec<ExecutableEntry>,
    // Other fields omitted for brevity
}

#[derive(Debug, Clone, Deserialize)]
pub struct ExecutableEntry {
    pub name: String,
    pub os: String,
    pub is_launcher: bool,
    pub arguments: Option<String>,
}

pub struct GameDatabase {
    entries: Vec<GameEntry>,
    linux_entries: Vec<GameEntry>, // Filtered cache
}
```

**Key Functions**:
- `load_from_file(path: &Path) -> Result<GameDatabase, DatabaseError>`
- `find_match(&self, path_info: &ProcessedPath, args: &[String]) -> Option<&GameEntry>`
- `is_executable_match(&self, exec: &ExecutableEntry, variants: &[String], args: &[String]) -> bool`

**Matching Algorithm**:
1. **OS Filtering**: Only consider executables with `os: "linux"`
2. **Launcher Filtering**: Skip entries with `is_launcher: true`
3. **Path Matching**: 
   - **Exact Match**: Check if `executable.name` equals any path variant
   - **Prefix Match**: Handle `>` prefix for process name matching
4. **Argument Matching**: If `arguments` field exists, verify process args contain the pattern

**Special Cases**:
- **Java Applications**: Handle `>java` and `>javaw.exe` patterns with argument matching
- **Argument Validation**: Use `String::contains()` for argument pattern matching
- **Case Sensitivity**: All matching is case-insensitive due to normalization

### 4. Activity Manager Module

**File**: `src/process/activity_manager.rs`

```rust
pub struct ActivityManager {
    active_games: HashMap<String, ActiveGame>,
    message_sender: mpsc::Sender<RpcMessage>,
}

#[derive(Debug)]
struct ActiveGame {
    game_id: String,
    game_name: String,
    pid: u32,
    start_timestamp: u64,
}

#[derive(Debug)]
pub struct RpcMessage {
    pub socket_id: String,
    pub command: RpcCommand,
}

#[derive(Debug)]
pub enum RpcCommand {
    SetActivity {
        activity: Option<Activity>,
        pid: u32,
    },
}
```

**Key Functions**:
- `update_detected_games(&mut self, detected: Vec<(GameEntry, u32)>)`
- `handle_new_game(&mut self, game: &GameEntry, pid: u32)`
- `handle_lost_game(&mut self, game_id: &str)`
- `generate_activity(&self, game: &ActiveGame) -> Activity`

**Activity Lifecycle**:
1. **Detection**: New game found in scan
2. **Registration**: Add to `active_games` with current timestamp
3. **Activity Generation**: Create Discord activity with `start_timestamp`
4. **Continuous Updates**: Resend activity on each scan (intentional behavior)
5. **Cleanup**: Remove when game no longer detected

**Activity Structure**:
```rust
#[derive(Debug, Serialize)]
pub struct Activity {
    pub application_id: String,
    pub name: String,
    pub timestamps: Timestamps,
}

#[derive(Debug, Serialize)]
pub struct Timestamps {
    pub start: u64, // Unix timestamp in milliseconds
}
```

### 5. Main Process Detection Service

**File**: `src/process/detector.rs`

```rust
pub struct ProcessDetector {
    scanner: ProcessScanner,
    path_processor: PathProcessor,
    database: GameDatabase,
    activity_manager: ActivityManager,
    scan_interval: Duration,
}
```

**Key Functions**:
- `new(database_path: &Path, message_sender: mpsc::Sender<RpcMessage>) -> Result<Self, DetectorError>`
- `start(&mut self) -> JoinHandle<()>`
- `scan_cycle(&mut self) -> Result<(), DetectorError>`
- `stop(&mut self)`

**Main Loop Algorithm**:
```rust
async fn run_detection_loop(&mut self) {
    let mut interval = tokio::time::interval(self.scan_interval);
    
    loop {
        interval.tick().await;
        
        match self.scan_cycle() {
            Ok(_) => {
                // Log successful scan
            }
            Err(e) => {
                error!("Scan cycle failed: {}", e);
                // Continue running despite errors
            }
        }
    }
}
```

**Scan Cycle Steps**:
1. **Process Enumeration**: Get all running processes
2. **Path Processing**: Normalize and generate variants for each process
3. **Database Matching**: Find matching games in database
4. **Activity Updates**: Update activity manager with current state
5. **Performance Logging**: Track scan duration for monitoring

### 6. Error Handling

**File**: `src/process/error.rs`

```rust
#[derive(Debug, thiserror::Error)]
pub enum ProcessError {
    #[error("Failed to read /proc directory: {0}")]
    ProcReadError(#[from] io::Error),
    
    #[error("Failed to parse PID: {0}")]
    PidParseError(String),
    
    #[error("Failed to read cmdline for PID {pid}: {source}")]
    CmdlineReadError { pid: u32, source: io::Error },
}

#[derive(Debug, thiserror::Error)]
pub enum DatabaseError {
    #[error("Failed to load database from {path}: {source}")]
    LoadError { path: String, source: serde_json::Error },
    
    #[error("Database file not found: {0}")]
    FileNotFound(String),
}
```

**Error Recovery Strategies**:
- **Process Access Errors**: Skip inaccessible processes, continue scanning
- **Database Load Errors**: Fatal error, prevent service startup
- **Temporary I/O Errors**: Log and retry on next scan cycle
- **Permission Errors**: Expected for system processes, handle gracefully

### 7. Performance Considerations

**Optimization Strategies**:

1. **Database Filtering**:
   - Pre-filter Linux-only executables at startup
   - Cache filtered results for faster lookups
   - Use `Vec` instead of `HashMap` for small datasets

2. **Path Processing**:
   - Reuse `String` allocations where possible
   - Consider using `Cow<str>` for path variants
   - Limit variant generation to reasonable depth

3. **Process Scanning**:
   - Use parallel iteration for `/proc` reading
   - Implement process cache for unchanged PIDs
   - Skip scanning if system load is high

4. **Memory Management**:
   - Clear temporary collections after each scan
   - Use `Vec::with_capacity()` for known sizes
   - Consider using a memory pool for frequent allocations

**Performance Targets**:
- **Scan Duration**: < 50ms for typical desktop (100-500 processes)
- **Memory Usage**: < 10MB resident for detector service
- **CPU Usage**: < 1% average, < 5% during scans

### 8. Configuration

**File**: `src/process/config.rs`

```rust
#[derive(Debug, Clone, Deserialize)]
pub struct ProcessConfig {
    pub scan_interval_ms: u64,
    pub database_path: PathBuf,
    pub proc_path: PathBuf,
    pub enable_performance_logging: bool,
    pub max_path_variants: usize,
}

impl Default for ProcessConfig {
    fn default() -> Self {
        Self {
            scan_interval_ms: 5000,
            database_path: PathBuf::from("detectable.json"),
            proc_path: PathBuf::from("/proc"),
            enable_performance_logging: false,
            max_path_variants: 20,
        }
    }
}
```

### 9. Integration with RPC Server

**Communication Channel**:
- Use `tokio::sync::mpsc` for async message passing
- Send `RpcMessage` structs containing activity updates
- Handle backpressure with bounded channels

**Message Format**:
```rust
pub struct RpcMessage {
    pub socket_id: String, // Game ID used as socket identifier
    pub command: RpcCommand,
}

// Matches the Node.js format:
// { cmd: 'SET_ACTIVITY', args: { activity: {...}, pid: 1234 } }
```

**Error Propagation**:
- Non-blocking sends to avoid stalling detection
- Log dropped messages due to channel overflow
- Implement health checks for RPC server connectivity

### 10. Testing Strategy

**Unit Tests**:
- Path normalization with various input formats
- Database matching with edge cases
- Activity generation and lifecycle management
- Error handling for common failure scenarios

**Integration Tests**:
- Full scan cycle with mock `/proc` filesystem
- Database loading with malformed JSON
- RPC message generation and formatting
- Performance tests with large process lists

**Test Data**:
- Create mock `/proc` entries for common games
- Test with empty, malformed, and permission-denied scenarios
- Validate against known game installations

### 11. Logging and Monitoring

**Log Levels**:
- **INFO**: Game detection/loss events, startup/shutdown
- **DEBUG**: Scan performance, match attempts
- **WARN**: Recoverable errors, permission issues
- **ERROR**: Fatal errors, database load failures

**Metrics**:
- Scan duration and frequency
- Number of processes scanned
- Number of games detected
- Error rates by type

**Log Format**:
```
[2024-01-15T10:30:45Z INFO  arrpc::process] Game detected: Counter-Strike: Global Offensive (PID: 12345)
[2024-01-15T10:30:50Z DEBUG arrpc::process] Scan completed in 23ms, found 247 processes
[2024-01-15T10:31:15Z INFO  arrpc::process] Game lost: Counter-Strike: Global Offensive
```

## Implementation Timeline

### Phase 1: Core Infrastructure (Week 1-2)
- [ ] Process scanner implementation
- [ ] Path processing module
- [ ] Basic error handling
- [ ] Unit tests for core functions

### Phase 2: Game Detection (Week 3-4)
- [ ] Database loading and parsing
- [ ] Matching algorithm implementation
- [ ] Activity manager
- [ ] Integration tests

### Phase 3: Service Integration (Week 5)
- [ ] RPC message generation
- [ ] Configuration management
- [ ] Logging and monitoring
- [ ] Performance optimization

### Phase 4: Testing and Polish (Week 6)
- [ ] Comprehensive test suite
- [ ] Performance benchmarking
- [ ] Documentation
- [ ] Code review and refinement

## Dependencies

**Required Crates**:
```toml
[dependencies]
tokio = { version = "1.0", features = ["full"] }
serde = { version = "1.0", features = ["derive"] }
serde_json = "1.0"
thiserror = "1.0"
tracing = "0.1"
tracing-subscriber = "0.3"
```

**Development Dependencies**:
```toml
[dev-dependencies]
tempfile = "3.0"
criterion = "0.5"
```

## Security Considerations

1. **Process Access**: Handle permission errors gracefully, don't escalate privileges
2. **Path Traversal**: Validate `/proc` paths to prevent directory traversal
3. **Resource Limits**: Implement timeouts for file operations
4. **Input Validation**: Sanitize process names and arguments before processing

## Compatibility Notes

**Linux Distributions**:
- Tested on Ubuntu 20.04+, Fedora 35+, Arch Linux
- Requires `/proc` filesystem (standard on all modern Linux)
- No additional privileges required beyond normal user access

**Performance Characteristics**:
- Scales linearly with number of running processes
- Memory usage proportional to number of detected games
- CPU usage spikes during scans, idle between scans

This implementation plan provides a solid foundation for building a robust, efficient process detection system in Rust that matches the functionality of the existing Node.js implementation while providing better performance and type safety.