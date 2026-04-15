**Paro Logging System**

A high-performance, configurable logging system built on `tracing`.

---

**Quick Start**

**Simple Initialization (Console Only)**

```rust
use paro_common::logging;
use paro_common::config::LoggingConfig;

// Option 1: Use default config (INFO level, console output)
logging::init_default();

// Option 2: Use custom config
let config = LoggingConfig::default();
logging::init(&config);

// Option 3: Use environment variable (RUST_LOG)
logging::init_from_env();
```

**Advanced Initialization (With LogManager)**

Use `LogManager` for runtime configuration changes and SQL log querying.

```rust
use paro_common::logging::LogManager;
use paro_common::config::LoggingConfig;

let log_manager = LogManager::init(LoggingConfig::default())?;

// Change log level at runtime
log_manager.set_level(LogLevel::Debug)?;

// Set custom filter
log_manager.set_filter("paro::query=trace,paro::storage=debug")?;

// Query stored logs programmatically
let entries = log_manager.memory_storage().all();
```

---

**Basic Usage**

No logger instance needed! Just use `tracing` macros anywhere:

```rust
use tracing::{info, debug, warn, error, trace};

fn my_function() {
    info!("Simple message");
    debug!("Debug with value: {}", some_value);
    info!(user_id = 123, "With structured fields");
    warn!(duration_ms = 500, "Slow query detected");
    error!(error = %e, "Operation failed");
}
```

---

**Using Predefined Targets**

Targets help categorize logs for filtering:

```rust
use tracing::info;
use paro_common::logging::targets;

info!(target: targets::QUERY, query_id = 1, "Query started");
info!(target: targets::STORAGE, "Checkpoint completed");
info!(target: targets::CONNECTION, conn_id = 5, "Client connected");
```

**Available Targets:**

| Target | Description |
|--------|-------------|
| `targets::QUERY` | Query execution |
| `targets::PARSER` | SQL parsing |
| `targets::PLANNER` | Query planning |
| `targets::OPTIMIZER` | Query optimization |
| `targets::EXECUTOR` | Query execution |
| `targets::STORAGE` | Storage engine |
| `targets::WAL` | Write-ahead log |
| `targets::TRANSACTION` | Transactions |
| `targets::CATALOG` | Catalog/metadata |
| `targets::CONNECTION` | Client connections |
| `targets::SESSION` | Session management |
| `targets::SERVER` | Server operations |
| `targets::INSTANCE` | Paro runtime instance (scheduler, DB manager, etc.) |
| `targets::BUFFER` | Buffer pool |

---

**Convenience Macros**

These macros automatically use the module path as the target:

```rust
use paro_common::{paro_info, paro_debug, paro_warn, paro_error, paro_trace};

paro_info!("Server started");
paro_debug!(user_id = 123, "Processing request");
paro_error!("Connection failed: {}", err);
```

---

**Configuration**

**LoggingConfig Fields:**

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | bool | true | Enable/disable logging |
| `level` | LogLevel | Info | Minimum log level |
| `format` | LogFormat | Pretty | Output format |
| `file` | Option<PathBuf> | None | Log file path |
| `rotation` | Option<RotationConfig> | None | File rotation config |
| `with_target` | bool | true | Show target in output |
| `with_file` | bool | false | Show source file |
| `with_line_number` | bool | false | Show line number |
| `ansi` | bool | true | Use ANSI colors |

**Log Levels:**

- `Trace` - Most verbose
- `Debug` - Debug information
- `Info` - General information (default)
- `Warn` - Warnings
- `Error` - Errors only

**Log Formats:**

- `Pretty` - Human-readable, multi-line (default)
- `Compact` - Single-line format
- `Json` - JSON structured format

---

**File Logging with Rotation**

```rust
use std::path::PathBuf;
use paro_common::config::{LoggingConfig, RotationConfig, RotationPolicy};

let config = LoggingConfig {
    file: Some(PathBuf::from("/var/log/paro/paro.log")),
    rotation: Some(RotationConfig {
        policy: RotationPolicy::Daily,
        max_files: Some(7),
        max_size: None,
    }),
    ..Default::default()
};

logging::init(&config);
```

---

**Environment Variable Override**

Use `RUST_LOG` to override log levels:

```bash
# Set global level
RUST_LOG=debug cargo run

# Fine-grained control
RUST_LOG="paro::query=trace,paro::storage=debug" cargo run

# Multiple targets
RUST_LOG="warn,paro=info,paro::query=debug" cargo run
```

---

**Querying Logs via SQL**

When using `LogManager`, logs are stored in memory and can be queried:

```sql
-- Query recent logs
SELECT * FROM paro_logs() LIMIT 100;

-- Filter by level
SELECT * FROM paro_logs() WHERE level = 'ERROR';

-- Filter by target
SELECT * FROM paro_logs() WHERE target LIKE 'paro::query%';

-- Get connection logs
SELECT timestamp, message FROM paro_logs() 
WHERE target = 'paro::connection' 
ORDER BY timestamp DESC;
```

---

**Programmatic Log Queries**

```rust
use paro_common::logging::{LogManager, LogQueryFilter, LogLevel};

let manager = LogManager::init(config)?;

// Query with filters
let filter = LogQueryFilter::new()
    .with_min_level(LogLevel::Warn)
    .with_target_prefix("paro::query")
    .with_limit(100);

let entries = manager.memory_storage().query(&filter);

for entry in entries {
    println!("[{}] {}: {}", entry.level, entry.target, entry.message);
}
```

---

**Module Structure**

```
paro-common/src/logging/
├── mod.rs          # Public API exports
├── init.rs         # Simple initialization functions
├── manager.rs      # LogManager for runtime control
├── storage.rs      # In-memory log storage
├── layer.rs        # Custom tracing layer
├── macros.rs       # Convenience macros
└── targets.rs      # Predefined log targets
```

---

**Best Practices**

1. **Use structured fields** for machine-parseable logs:
   ```rust
   info!(user_id = 123, action = "login", "User logged in");
   ```

2. **Use targets** for component filtering:
   ```rust
   info!(target: targets::QUERY, "Query completed");
   ```

3. **Use spans** for context propagation:
   ```rust
   let _span = tracing::info_span!("process_request", request_id = %id).entered();
   info!("Processing...");  // Inherits span context
   ```

4. **Use `LogManager`** in production for runtime control and log querying.

5. **Configure file logging** with rotation for production deployments.
