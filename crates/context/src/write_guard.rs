use paro_common::error::{self as paro_error, Result};
use std::sync::Mutex;

use crate::WriteClass;

#[derive(Debug, Default)]
pub struct WriteGuard {
    class: Mutex<WriteClass>,
}

impl WriteGuard {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn begin_dml_write(&self) -> Result<()> {
        let mut class = self
            .class
            .lock()
            .map_err(|_| paro_error::internal("txn write state poisoned"))?;
        match *class {
            WriteClass::Clean | WriteClass::HasDml => {
                *class = WriteClass::HasDml;
                Ok(())
            }
            WriteClass::HasDdl | WriteClass::HasDmlAndDdl => {
                *class = WriteClass::HasDmlAndDdl;
                Ok(())
            }
        }
    }

    pub fn begin_object_ddl(&self) -> Result<()> {
        let mut class = self
            .class
            .lock()
            .map_err(|_| paro_error::internal("txn write state poisoned"))?;
        match *class {
            WriteClass::Clean | WriteClass::HasDdl => {
                *class = WriteClass::HasDdl;
                Ok(())
            }
            WriteClass::HasDml | WriteClass::HasDmlAndDdl => {
                *class = WriteClass::HasDmlAndDdl;
                Ok(())
            }
        }
    }

    pub fn class(&self) -> WriteClass {
        self.class
            .lock()
            .map(|guard| *guard)
            .unwrap_or(WriteClass::HasDmlAndDdl)
    }

    pub fn mark(&self) -> WriteClass {
        self.class()
    }

    pub fn restore(&self, mark: WriteClass) {
        if let Ok(mut class) = self.class.lock() {
            *class = mark;
        }
    }

    pub fn reset(&self) {
        if let Ok(mut class) = self.class.lock() {
            *class = WriteClass::Clean;
        }
    }
}
