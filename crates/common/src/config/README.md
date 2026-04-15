**Paro Configuration System**

A layered configuration system using `figment` + `serde` + `toml`.

---

**Quick Start**

**Loading Configuration**

```rust
use paro_common::config::{ConfigLoader, ParoConfig};

// Load from all sources (files, env vars)
let config = ConfigLoader::load()?;

// Load with custom config file
let config = ConfigLoader::load_with_options(Some("/path/to/paro.toml".into()))?;

// Load from string (for testing)
let config = ConfigLoader::load_from_str(r#"
    [server]
    port = 5432
"#)?;
```

---

**Configuration Priority**

Configuration is merged from multiple sources (low to high priority):

1. **Code defaults** - `Default` trait implementations
2. **System config** - `/etc/paro/paro.toml`
3. **User config** - `~/.config/paro/paro.toml`
4. **Local config** - `./paro.toml`
5. **Custom file** - `--config path`
6. **Environment variables** - `PARO_*`

Higher priority sources override lower priority ones.

---

**Configuration Sections**

**ParoConfig** (Root)

```rust
pub struct ParoConfig {
    pub server: ServerConfig,
    pub cluster: ClusterConfig,
    pub logging: LoggingConfig,
    pub storage: StorageConfig,
}
```

---

**Server Configuration**

```toml
[server]
host = "0.0.0.0"          # Listen address
port = 6432               # Listen port
max_connections = 0       # 0 = unlimited
allow_plaintext = false   # Require an explicit override before ignoring [server.tls]
# copy_stdin_memory_limit = "256MiB"  # Optional COPY FROM STDIN buffer cap

[server.tls]              # Optional TLS (not yet implemented)
cert = "/path/to/cert.pem"
key = "/path/to/key.pem"
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `host` | String | "0.0.0.0" | Listen address |
| `port` | u16 | 6432 | Listen port |
| `max_connections` | usize | 0 | Max connections (0 = unlimited) |
| `allow_plaintext` | bool | false | Permit plaintext startup when `tls` is configured but unsupported |
| `copy_stdin_memory_limit` | Option<String/usize> | auto | COPY FROM STDIN buffer cap; defaults to `min(cluster.max_memory / 4, 1GiB)` |
| `tls` | Option | None | TLS configuration |

---

**Cluster Configuration**

```toml
[cluster]
max_memory = "2GiB"              # Human-readable size
num_threads = 4                  # Worker threads (omit for auto)
default_database = "postgres"    # Default database name
access_mode = "read_write"       # read_write or read_only
enable_external_access = true    # Allow file/network access
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `max_memory` | String/usize | "1GiB" | Max memory (e.g., "2GiB", "512MiB") |
| `num_threads` | Option<usize> | None | Worker threads (None = auto) |
| `default_database` | String | "postgres" | Default database |
| `access_mode` | AccessMode | read_write | Access mode |
| `enable_external_access` | bool | true | Enable external access |

---

**Logging Configuration**

```toml
[logging]
enabled = true
level = "info"                   # trace, debug, info, warn, error
format = "pretty"                # pretty, compact, json
file = "/var/log/paro/paro.log"  # Optional log file
with_target = true
with_file = false
with_line_number = false
ansi = true

[logging.rotation]
policy = "daily"                 # never, daily, hourly
max_files = 7
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | bool | true | Enable logging |
| `level` | LogLevel | info | Minimum log level |
| `format` | LogFormat | pretty | Output format |
| `file` | Option<PathBuf> | None | Log file path |
| `rotation` | Option | None | File rotation config |
| `with_target` | bool | true | Show target in logs |
| `ansi` | bool | true | Use ANSI colors |

---

**Storage Configuration**

```toml
[storage]
data_dir = "./data"              # Data directory

[storage.buffer_pool]
size = 1024                      # Buffer pool size (pages)

[storage.checkpoint]
interval = "5m"                  # Checkpoint interval
wal_size = "64MiB"              # WAL size threshold
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `data_dir` | PathBuf | "./data" | Data directory |
| `buffer_pool.size` | usize | 1024 | Buffer pool size |
| `checkpoint.interval` | Duration | 5m | Checkpoint interval |
| `checkpoint.wal_size` | String | "64MiB" | WAL size threshold |

---

**Environment Variables**

Environment variables override config files. Use double underscore for nested keys:

```bash
# Server
export PARO_SERVER__HOST=127.0.0.1
export PARO_SERVER__PORT=5432

# Cluster
export PARO_CLUSTER__MAX_MEMORY=4294967296   # 4GB in bytes
export PARO_CLUSTER__NUM_THREADS=8

# Logging
export PARO_LOGGING__LEVEL=debug
export PARO_LOGGING__FORMAT=json

# Storage
export PARO_STORAGE__DATA_DIR=/var/lib/paro
```

---

**Sample Configuration File**

Generate a sample config file:

```rust
let sample = ConfigLoader::sample_config();
println!("{}", sample);
```

Or from command line:

```bash
parod --print-config > paro.toml
```

---

**Human-Readable Values**

The config system supports human-readable values for sizes and durations:

**Sizes:**
- `"512MiB"`, `"2GiB"`, `"100MB"`, `"1TB"`
- Binary units (MiB, GiB, TiB) use powers of 1024
- Decimal units (MB, GB, TB) use powers of 1000

**Durations:**
- `"30s"`, `"5m"`, `"1h"`, `"1d"`
- Supported: `s` (seconds), `m` (minutes), `h` (hours), `d` (days)

---

**Programmatic Access**

```rust
use paro_common::config::{ConfigLoader, ParoConfig};

let config = ConfigLoader::load()?;

// Access server config
println!("Listening on {}", config.server.address());

// Access cluster config
println!("Max memory: {} bytes", config.cluster.max_memory);
println!("Threads: {:?}", config.cluster.num_threads);

// Access logging config
println!("Log level: {:?}", config.logging.level);

// Access storage config
println!("Data dir: {}", config.storage.data_dir.display());
```

---

**Validation**

Configuration is validated after loading:

```rust
use paro_common::config::{ConfigLoader, validation};

let config = ConfigLoader::load()?;

// Validate the entire config
validation::validate_config(&config)?;

// Validate specific sections
validation::validate_server_config(&config.server)?;
validation::validate_cluster_config(&config.cluster)?;
```

Validation checks include:
- Port range (1-65535)
- Memory limits (minimum required)
- Data directory existence
- TLS certificate paths

---

**Module Structure**

```
paro-common/src/config/
├── mod.rs          # Public API exports
├── types.rs        # Configuration structs
├── loader.rs       # ConfigLoader implementation
├── validation.rs   # Validation functions
├── human_bytes.rs  # Human-readable byte parsing
└── sample_config.toml  # Sample configuration
```

---

**Complete Example**

```toml
# paro.toml - Complete configuration example

[server]
host = "0.0.0.0"
port = 6432
max_connections = 100

[cluster]
max_memory = "4GiB"
num_threads = 8
default_database = "postgres"
access_mode = "read_write"
enable_external_access = true

[logging]
enabled = true
level = "info"
format = "pretty"
file = "/var/log/paro/paro.log"
with_target = true
ansi = false

[logging.rotation]
policy = "daily"
max_files = 7

[storage]
data_dir = "/var/lib/paro/data"

[storage.buffer_pool]
size = 4096

[storage.checkpoint]
interval = "5m"
wal_size = "128MiB"
```

---

**Best Practices**

1. **Use environment variables** for secrets and deployment-specific values
2. **Use config files** for stable, shared settings
3. **Use human-readable formats** for memory sizes and durations
4. **Validate configuration** early at startup
5. **Use `--print-config`** to generate a documented template
