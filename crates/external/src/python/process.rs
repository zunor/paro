// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Own the embedded service source, its interpreter process and liveness pipe.

use std::process::Stdio;
use std::time::Duration;

use paro_common::error::{self as error, Result};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, BufReader};
use tokio::process::{Child, Command};

pub struct Service {
    pub child: Child,
    pub token: String,
    pub epoch: String,
    pub port: u16,
    liveness: Option<tokio::process::ChildStdin>,
    _directory: tempfile::TempDir,
}

macro_rules! source {
    ($name:literal) => {
        (
            $name,
            include_str!(concat!(
                "../../../../runtimes/python-worker/src/paro_worker/",
                $name
            )),
        )
    };
}

impl Service {
    pub async fn start() -> Result<Self> {
        let directory = tempfile::Builder::new().prefix("paro-python-").tempdir()?;
        let package = directory.path().join("paro_worker");
        std::fs::create_dir(&package)?;
        for (name, source) in [
            source!("__init__.py"),
            source!("__main__.py"),
            source!("protocol.py"),
            source!("definition.py"),
            source!("handler.py"),
            source!("process.py"),
            source!("watchdog.py"),
            source!("server.py"),
        ] {
            std::fs::write(package.join(name), source)?;
        }
        let sdk = directory.path().join("paro_udf");
        std::fs::create_dir(&sdk)?;
        for (name, source) in [
            (
                "__init__.py",
                include_str!("../../../../python/paro_udf/src/paro_udf/__init__.py"),
            ),
            (
                "context.py",
                include_str!("../../../../python/paro_udf/src/paro_udf/context.py"),
            ),
        ] {
            std::fs::write(sdk.join(name), source)?;
        }
        let mut random = [0; 32];
        getrandom::fill(&mut random).map_err(|e| error::worker_failure(e.to_string()))?;
        let token = random
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>();
        let epoch = token[..16].to_string();
        let mut child = Command::new(crate::runtime::host::default_python_binary())
            .args(["-u", "-m", "paro_worker"])
            .env("PYTHONPATH", directory.path())
            .env("PARO_WORKER_TOKEN", &token)
            .env("PARO_WORKER_EPOCH", &epoch)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .kill_on_drop(true)
            .spawn()
            .map_err(|e| error::worker_failure(format!("start Python Flight worker: {e}")))?;
        let liveness = child.stdin.take();
        let mut line = String::new();
        let startup = tokio::time::timeout(
            Duration::from_secs(15),
            BufReader::new(child.stdout.take().unwrap().take(4097)).read_line(&mut line),
        )
        .await;
        let port = match startup {
            Ok(Ok(size)) if size <= 4096 => serde_json::from_str::<serde_json::Value>(&line)
                .ok()
                .and_then(|v| v["port"].as_u64())
                .and_then(|p| u16::try_from(p).ok()),
            _ => None,
        };
        let Some(port) = port else {
            let _ = child.kill().await;
            let _ = child.wait().await;
            return Err(error::worker_failure(
                "Python Flight worker failed readiness",
            ));
        };
        Ok(Self {
            child,
            token,
            epoch,
            port,
            liveness,
            _directory: directory,
        })
    }

    pub async fn stop(&mut self) {
        self.liveness.take();
        if tokio::time::timeout(Duration::from_secs(5), self.child.wait())
            .await
            .is_err()
        {
            let _ = self.child.kill().await;
            let _ = self.child.wait().await;
        }
    }
}
