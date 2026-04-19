# Paro 配置系统设计文档

## 概述

本文档描述 Paro 数据库的配置系统设计方案。目标是提供一个统一的、分层的配置管理系统，支持配置文件、环境变量和命令行参数。

## 现状分析

### 现有配置结构

Paro 已有多个配置结构，但缺乏统一的加载机制：

| 配置结构 | 位置 | 用途 |
|---------|------|------|
| `ServerConfig` | `paro-server` | 服务器地址、端口、数据目录 |
| `ClusterConfig` | `paro-instance`（`InstanceConfig` 由 TOML `cluster` 节转换） | 内存限制、线程数、访问模式 |
| `SessionConfig` | `paro-session` | Profiler、优化器、变量 |
| `ExecutorConfig` | `paro-execution` | 执行器线程数 |
| `ParallelConfig` | `paro-execution` | 并行执行配置 |

### 现有问题

1. 配置硬编码（如 `"0.0.0.0:6432"`）
2. 没有配置文件加载机制
3. 没有命令行参数解析
4. 没有环境变量支持
5. 各配置结构独立，没有统一入口

Paro requires configuration file support for server deployment scenarios, while also supporting SQL SET commands for session-level settings.

## 推荐方案

### 技术选型：figment + serde + clap

| 组件 | 用途 | 特点 |
|------|------|------|
| **figment** | 分层配置管理 | Rocket 框架使用，成熟可靠 |
| **serde** | 序列化/反序列化 | Rust 生态标准 |
| **toml** | 配置文件格式 | 简洁易读，Rust 生态首选 |
| **clap** | 命令行参数 | 功能强大，derive 宏简洁 |

### 为什么选 figment 而不是 config-rs？

| 特性 | figment | config-rs |
|------|---------|-----------|
| 分层配置 | ✓ 内置 | ✓ 内置 |
| Serde 集成 | ✓ 原生支持 | ✓ 支持 |
| 类型安全 | ✓ 编译时检查 | 运行时检查 |
| 错误信息 | 精确到字段 | 较模糊 |
| 维护状态 | Rocket 团队维护 | 社区维护 |
| 命令行集成 | figment-clap | 需自己实现 |

## 设计方案

### 配置加载优先级（低到高）

```
1. 代码默认值 (Default trait)
     ↓ 覆盖
2. 配置文件 (~/.config/paro/paro.toml 或 /etc/paro/paro.toml)
     ↓ 覆盖
3. 环境变量 (PARO_*)
     ↓ 覆盖
4. 命令行参数 (--server.port 6432)
```

后面的配置覆盖前面的，实现灵活的配置方式。

### 统一配置结构

```rust
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// Paro 统一配置
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ParoConfig {
    /// 服务器配置
    pub server: ServerConfig,
    /// 集群配置
    pub cluster: ClusterConfig,
    /// 日志配置
    pub logging: LoggingConfig,
    /// 存储配置
    pub storage: StorageConfig,
}

/// 服务器配置
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ServerConfig {
    /// 监听地址
    pub host: String,
    /// 监听端口
    pub port: u16,
    /// 最大连接数 (0 = 无限制)
    pub max_connections: usize,
    /// TLS 配置
    pub tls: Option<TlsConfig>,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            host: "0.0.0.0".to_string(),
            port: 6432,
            max_connections: 0,
            tls: None,
        }
    }
}

/// 集群配置
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ClusterConfig {
    /// 最大内存 (bytes 或 "2GB" 格式)
    #[serde(with = "human_bytes")]
    pub max_memory: usize,
    /// 工作线程数 (None = 自动检测)
    pub num_threads: Option<usize>,
    /// 默认数据库名
    pub default_database: String,
    /// 访问模式
    pub access_mode: AccessMode,
    /// 是否允许外部访问
    pub enable_external_access: bool,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            max_memory: 1024 * 1024 * 1024, // 1GB
            num_threads: None,
            default_database: "postgres".to_string(),
            access_mode: AccessMode::ReadWrite,
            enable_external_access: true,
        }
    }
}

/// 日志配置
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct LoggingConfig {
    /// 日志级别
    pub level: LogLevel,
    /// 日志格式
    pub format: LogFormat,
    /// 日志文件路径 (None = 仅控制台)
    pub file: Option<PathBuf>,
    /// 日志文件滚动策略
    pub rotation: Option<RotationPolicy>,
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            level: LogLevel::Info,
            format: LogFormat::Pretty,
            file: None,
            rotation: None,
        }
    }
}

/// 存储配置
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct StorageConfig {
    /// 数据目录
    pub data_dir: PathBuf,
    /// 临时目录
    pub temp_dir: Option<PathBuf>,
    /// WAL 配置
    pub wal: WalConfig,
    /// 缓冲池配置
    pub buffer_pool: BufferPoolConfig,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("./data"),
            temp_dir: None,
            wal: WalConfig::default(),
            buffer_pool: BufferPoolConfig::default(),
        }
    }
}

/// 访问模式
#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum AccessMode {
    #[default]
    ReadWrite,
    ReadOnly,
}

/// 日志级别
#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum LogLevel {
    Trace,
    Debug,
    #[default]
    Info,
    Warn,
    Error,
}

/// 日志格式
#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum LogFormat {
    #[default]
    Pretty,
    Json,
    Compact,
}
```

### 配置文件格式 (paro.toml)

```toml
# Paro Database Configuration
# 配置文件搜索路径:
#   1. 命令行指定: --config /path/to/paro.toml
#   2. 当前目录: ./paro.toml
#   3. 用户目录: ~/.config/paro/paro.toml
#   4. 系统目录: /etc/paro/paro.toml

[server]
host = "0.0.0.0"
port = 6432
max_connections = 100

# TLS 配置 (可选)
# [server.tls]
# cert = "/path/to/cert.pem"
# key = "/path/to/key.pem"

[cluster]
# 内存限制，支持人类可读格式
max_memory = "2GB"
# 线程数，不设置则自动检测 CPU 核心数
# num_threads = 8
default_database = "postgres"
access_mode = "read_write"
enable_external_access = true

[logging]
level = "info"
format = "pretty"    # pretty, json, compact
# file = "/var/log/paro/paro.log"
# [logging.rotation]
# policy = "daily"   # daily, hourly, size
# max_files = 7

[storage]
data_dir = "/var/lib/paro/data"
# temp_dir = "/tmp/paro"

[storage.wal]
enabled = true

[storage.checkpoint]
trigger_bytes = "16MiB"
trigger_interval = "5m"
drain_timeout = "30s"
max_concurrent_writers = 4
artifact_gc_batch_size = 64
artifact_gc_delete_budget = 256
checkpoint_gc_delete_budget = 8
segment_prune_delete_budget = 32

[storage.buffer_pool]
size = "512MB"
```

### 配置加载器

```rust
use figment::{
    providers::{Env, Format, Serialized, Toml},
    Figment, Profile,
};
use clap::Parser;
use std::path::PathBuf;

/// 配置加载器
pub struct ConfigLoader;

impl ConfigLoader {
    /// 加载配置 (合并所有来源)
    pub fn load() -> Result<ParoConfig, ConfigError> {
        let cli = CliArgs::parse();
        
        // 构建分层配置
        let figment = Figment::new()
            // 1. 默认值
            .merge(Serialized::defaults(ParoConfig::default()))
            // 2. 系统配置文件
            .merge(Toml::file("/etc/paro/paro.toml").nested())
            // 3. 用户配置文件
            .merge(Toml::file(Self::user_config_path()).nested())
            // 4. 当前目录配置文件
            .merge(Toml::file("paro.toml").nested())
            // 5. 命令行指定的配置文件
            .merge(Self::cli_config_file(&cli))
            // 6. 环境变量 (PARO_SERVER__PORT=6432)
            .merge(Env::prefixed("PARO_").split("__"))
            // 7. 命令行参数
            .merge(Serialized::defaults(&cli));
        
        figment.extract().map_err(ConfigError::from)
    }
    
    /// 仅从配置文件加载
    pub fn load_from_file(path: &PathBuf) -> Result<ParoConfig, ConfigError> {
        let figment = Figment::new()
            .merge(Serialized::defaults(ParoConfig::default()))
            .merge(Toml::file(path).nested());
        
        figment.extract().map_err(ConfigError::from)
    }
    
    /// 用户配置文件路径
    fn user_config_path() -> PathBuf {
        dirs::config_dir()
            .unwrap_or_else(|| PathBuf::from("."))
            .join("paro")
            .join("paro.toml")
    }
    
    fn cli_config_file(cli: &CliArgs) -> impl figment::Provider {
        match &cli.config {
            Some(path) => Toml::file(path).nested(),
            None => Toml::file("").nested(), // 空提供者
        }
    }
}

/// 命令行参数
#[derive(Parser, Debug, Clone, Serialize, Deserialize, Default)]
#[command(name = "parod")]
#[command(about = "Paro Database Server")]
pub struct CliArgs {
    /// 配置文件路径
    #[arg(short, long)]
    pub config: Option<PathBuf>,
    
    /// 监听地址
    #[arg(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub host: Option<String>,
    
    /// 监听端口
    #[arg(short, long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub port: Option<u16>,
    
    /// 数据目录
    #[arg(short, long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data_dir: Option<PathBuf>,
    
    /// 日志级别
    #[arg(short, long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub log_level: Option<LogLevel>,
}

/// 配置错误
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("Failed to load configuration: {0}")]
    Load(#[from] figment::Error),
    
    #[error("Invalid configuration: {0}")]
    Invalid(String),
    
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}
```

### 环境变量映射

```bash
# 服务器配置
PARO_SERVER__HOST=127.0.0.1
PARO_SERVER__PORT=6432
PARO_SERVER__MAX_CONNECTIONS=100

# 集群配置
PARO_CLUSTER__MAX_MEMORY=2147483648
PARO_CLUSTER__NUM_THREADS=8
PARO_CLUSTER__DEFAULT_DATABASE=mydb

# 日志配置
PARO_LOGGING__LEVEL=debug
PARO_LOGGING__FORMAT=json
PARO_LOGGING__FILE=/var/log/paro/paro.log

# 存储配置
PARO_STORAGE__DATA_DIR=/var/lib/paro
```

注意：环境变量使用双下划线 `__` 表示嵌套层级。

## 模块结构

```
paro-common/src/config/
├── mod.rs          # 公共导出
├── types.rs        # 配置结构定义
├── loader.rs       # 配置加载器
├── validation.rs   # 配置校验
└── human_bytes.rs  # 人类可读字节数解析 (如 "2GB")

paro-server/src/
├── bin/
│   └── parod.rs    # 使用 clap 解析命令行
└── cli.rs          # 命令行参数定义
```

## 使用示例

### 服务器启动

```rust
// crates/server/src/bin/parod.rs

use paro_common::config::{ConfigLoader, ParoConfig};
use tracing::info;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // 1. 加载配置（自动合并所有来源）
    let config = ConfigLoader::load()?;
    
    // 2. 初始化日志（使用配置）
    paro_common::logging::init_with_config(&config.logging);
    
    info!("Configuration loaded");
    info!("  Server: {}:{}", config.server.host, config.server.port);
    info!("  Data directory: {:?}", config.storage.data_dir);
    info!("  Max memory: {} bytes", config.cluster.max_memory);
    
    // 3. 启动服务器
    let addr = format!("{}:{}", config.server.host, config.server.port);
    paro_server::run_server_with_config(&addr, config).await?;
    
    Ok(())
}
```

### 命令行使用

```bash
# 使用默认配置
parod

# 指定配置文件
parod --config /etc/paro/production.toml

# 命令行覆盖
parod --port 5432 --log-level debug

# 环境变量覆盖
PARO_SERVER__PORT=5432 parod

# 混合使用（优先级：命令行 > 环境变量 > 配置文件 > 默认值）
PARO_LOGGING__LEVEL=trace parod --config prod.toml --port 5432
```

### 程序化使用

```rust
// 测试场景
let config = ParoConfig {
    cluster: ClusterConfig {
        max_memory: 100 * 1024 * 1024, // 100MB
        ..Default::default()
    },
    ..Default::default()
};

let cluster = Cluster::bootstrap_with_config(config.cluster)?;
```

## 与现有代码的整合

### Implementation Roadmap

The implementation follows a modular approach:
1.  **Core Configuration Module**: Add the `config` module in `paro-common`, define unified configuration structures, and implement the configuration loader.
2.  **Server Integration**: Update the server to use the new configuration system and add command-line argument support.
3.  **Cross-Module Migration**: Update other components (Cluster, Session, Logging) to source their settings from the unified configuration.

### 向后兼容

现有的 `XxxConfig::default()` 模式继续工作：

```rust
// 旧方式（仍然支持）
let cluster_config = ClusterConfig::default();

// 新方式（从文件加载）
let paro_config = ConfigLoader::load()?;
let cluster_config = paro_config.cluster;
```

## 依赖

```toml
# Cargo.toml

[dependencies]
serde = { version = "1.0", features = ["derive"] }
toml = "0.8"
figment = { version = "0.10", features = ["toml", "env"] }
clap = { version = "4.0", features = ["derive"] }
dirs = "5.0"
thiserror = "2.0"

# 可选：人类可读的字节数解析
bytesize = "1.3"
```

## 人类可读格式支持

### 字节数

```toml
max_memory = "2GB"      # 2147483648
buffer_size = "512MB"   # 536870912
cache_size = "64KB"     # 65536
```

### 时间

```toml
timeout = "30s"              # 30 seconds
checkpoint_trigger_interval = "5m"  # 5 minutes
checkpoint_drain_timeout = "30s"    # 30 seconds
max_age = "24h"              # 24 hours
```

### 实现

```rust
mod human_bytes {
    use serde::{Deserialize, Deserializer, Serializer};
    use bytesize::ByteSize;

    pub fn serialize<S>(bytes: &usize, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&ByteSize(*bytes as u64).to_string())
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<usize, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        // 尝试解析为人类可读格式
        if let Ok(size) = s.parse::<ByteSize>() {
            return Ok(size.as_u64() as usize);
        }
        // 尝试解析为纯数字
        s.parse::<usize>().map_err(serde::de::Error::custom)
    }
}
```

## 配置校验

```rust
impl ParoConfig {
    /// 校验配置有效性
    pub fn validate(&self) -> Result<(), ConfigError> {
        // 端口范围
        if self.server.port == 0 {
            return Err(ConfigError::Invalid("Server port cannot be 0".to_string()));
        }
        
        // 内存限制
        if self.cluster.max_memory < 64 * 1024 * 1024 {
            return Err(ConfigError::Invalid(
                "max_memory must be at least 64MB".to_string()
            ));
        }
        
        // 数据目录
        if self.storage.data_dir.as_os_str().is_empty() {
            return Err(ConfigError::Invalid(
                "data_dir cannot be empty".to_string()
            ));
        }
        
        Ok(())
    }
}
```

## 总结

### 方案优势

1. **分层配置**: 灵活的优先级覆盖
2. **类型安全**: 编译时检查配置结构
3. **人类可读**: 支持 "2GB", "30s" 等格式
4. **多来源支持**: 文件、环境变量、命令行
5. **统一入口**: 一个 `ParoConfig` 包含所有配置
6. **向后兼容**: 不破坏现有代码

### 与日志系统的关系

配置系统是日志系统的前置依赖：

```rust
fn main() {
    // 1. 先加载配置（此时还没有日志）
    let config = ConfigLoader::load().expect("Failed to load config");
    
    // 2. 用配置初始化日志
    paro_common::logging::init_with_config(&config.logging);
    
    // 3. 现在可以正常使用日志了
    info!("Paro server starting...");
}
```
