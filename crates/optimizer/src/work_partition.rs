//! Opt-in, exclusive wall-time accounting. Nested scopes suspend their parent.
//! Uninstrumented time remains explicit; this is not a CPU-time profiler.
use std::{cell::RefCell, marker::PhantomData, rc::Rc, time::Instant};

#[derive(Clone, Copy)]
#[repr(usize)]
pub enum Bucket {
    Pre,
    Agenda,
    Match,
    Apply,
    Insert,
    Schedule,
    Recipe,
    Subproblem,
    Kernel,
    Admission,
    Publish,
    Quality,
    Finish,
    QualityEvidence,
    QualityDomain,
    QualityProduction,
    QualityFreeze,
    QualityReads,
    Unclassified,
}
const N: usize = 19;
const NAMES: [&str; N] = [
    "B0_pre",
    "B1_agenda",
    "B2_match",
    "B3_apply",
    "B4_insert",
    "B5_schedule",
    "B6_recipe",
    "B7_subproblem",
    "B8_kernel",
    "B9_admission",
    "B10_publish",
    "B11_quality",
    "B12_finish",
    "B11_evidence",
    "B11_domain",
    "B11_production",
    "B11_freeze",
    "B11_reads",
    "unclassified",
];
struct Ledger {
    start: Instant,
    cursor: u64,
    current: usize,
    ns: [u64; N],
    entries: [u64; N],
}
impl Ledger {
    fn change(&mut self, now: u64, next: usize) -> usize {
        let previous = self.current;
        self.ns[previous] += now - self.cursor;
        self.cursor = now;
        self.current = next;
        previous
    }
    fn now(&self) -> u64 {
        self.start
            .elapsed()
            .as_nanos()
            .try_into()
            .unwrap_or(u64::MAX)
    }
}
#[derive(Default)]
struct Slot {
    generation: u64,
    ledger: Option<Ledger>,
}
thread_local! { static SLOT: RefCell<Slot> = RefCell::new(Slot::default()); }

pub struct Scope {
    previous: Option<(u64, usize)>,
    _not_send: PhantomData<Rc<()>>,
}
pub fn enter(bucket: Bucket) -> Scope {
    let previous = SLOT.with(|slot| {
        let mut slot = slot.borrow_mut();
        let generation = slot.generation;
        let ledger = slot.ledger.as_mut()?;
        let previous = ledger.change(ledger.now(), bucket as usize);
        ledger.entries[bucket as usize] += 1;
        Some((generation, previous))
    });
    Scope {
        previous,
        _not_send: PhantomData,
    }
}
impl Drop for Scope {
    fn drop(&mut self) {
        if let Some((generation, previous)) = self.previous {
            SLOT.with(|slot| {
                let mut slot = slot.borrow_mut();
                if slot.generation == generation {
                    if let Some(ledger) = slot.ledger.as_mut() {
                        ledger.change(ledger.now(), previous);
                    }
                }
            });
        }
    }
}
pub struct Invocation {
    generation: u64,
    _not_send: PhantomData<Rc<()>>,
}
pub fn begin(start: Instant) -> Invocation {
    let enabled = std::env::var_os("PARO_DIAGNOSTIC_WORK_PARTITION").is_some();
    let generation = SLOT.with(|slot| {
        let mut slot = slot.borrow_mut();
        slot.generation += 1;
        slot.ledger = enabled.then_some(Ledger {
            start,
            cursor: 0,
            current: Bucket::Unclassified as usize,
            ns: [0; N],
            entries: [0; N],
        });
        slot.generation
    });
    Invocation {
        generation,
        _not_send: PhantomData,
    }
}
pub struct Report {
    ledger: Ledger,
    total_ns: u64,
}
impl Invocation {
    pub fn finish(self, end: Instant) -> Option<Report> {
        SLOT.with(|slot| {
            let mut slot = slot.borrow_mut();
            if slot.generation != self.generation {
                return None;
            }
            slot.ledger.take().map(|mut ledger| {
                let total_ns = end
                    .duration_since(ledger.start)
                    .as_nanos()
                    .try_into()
                    .unwrap_or(u64::MAX);
                ledger.change(total_ns, Bucket::Unclassified as usize);
                Report { ledger, total_ns }
            })
        })
    }
}
impl Drop for Invocation {
    fn drop(&mut self) {
        SLOT.with(|slot| {
            let mut slot = slot.borrow_mut();
            if slot.generation == self.generation {
                slot.ledger = None;
            }
        });
    }
}
impl Report {
    /// Called after the optimizer interval, but inside diagnostic compiler/C1.
    pub fn write(self, statement: &str, success: bool) -> std::io::Result<()> {
        use std::io::Write;
        let Some(path) = std::env::var_os("PARO_DIAGNOSTIC_WORK_PARTITION") else {
            return Ok(());
        };
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)?;
        let mut writer = std::io::BufWriter::new(file);
        let buckets: Vec<_> = NAMES.iter().enumerate().map(|(i, name)| serde_json::json!({
            "bucket": name, "exclusive_ns": self.ledger.ns[i], "entries": self.ledger.entries[i],
        })).collect();
        serde_json::to_writer(
            &mut writer,
            &serde_json::json!({
                "pid": std::process::id(), "statement": statement, "success": success,
                "total_ns": self.total_ns, "sum_ns": self.ledger.ns.iter().sum::<u64>(), "buckets": buckets,
            }),
        )?;
        writeln!(writer)?;
        writer.flush()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn invocation() -> Invocation {
        let invocation = begin(Instant::now());
        SLOT.with(|slot| {
            slot.borrow_mut().ledger = Some(Ledger {
                start: Instant::now(),
                cursor: 0,
                current: N - 1,
                ns: [0; N],
                entries: [0; N],
            })
        });
        invocation
    }
    #[test]
    fn unwind_restores_parent_and_cancel_clears_ledger() {
        let invocation = invocation();
        let outer = enter(Bucket::Agenda);
        let result = std::panic::catch_unwind(|| {
            let _inner = enter(Bucket::Apply);
            panic!("cancel");
        });
        assert!(result.is_err());
        SLOT.with(|slot| {
            assert_eq!(
                slot.borrow().ledger.as_ref().unwrap().current,
                Bucket::Agenda as usize
            )
        });
        drop(outer);
        drop(invocation);
        SLOT.with(|slot| assert!(slot.borrow().ledger.is_none()));
    }
    #[test]
    fn old_scope_cannot_modify_new_invocation() {
        let old = invocation();
        let stale_scope = enter(Bucket::Apply);
        let new = invocation();
        drop(stale_scope);
        drop(old);
        let report = new.finish(Instant::now()).unwrap();
        assert_eq!(report.ledger.entries.iter().sum::<u64>(), 0);
        assert_eq!(report.ledger.ns.iter().sum::<u64>(), report.total_ns);
        assert_eq!(report.ledger.ns[N - 1], report.total_ns);
    }
    #[test]
    fn nested_intervals_partition_without_double_counting() {
        let mut ledger = Ledger {
            start: Instant::now(),
            cursor: 0,
            current: N - 1,
            ns: [0; N],
            entries: [0; N],
        };
        ledger.change(5, 5);
        ledger.change(12, 2);
        ledger.change(16, 5);
        ledger.change(20, N - 1);
        ledger.change(23, N - 1);
        assert_eq!(ledger.ns[N - 1], 8);
        assert_eq!(ledger.ns[5], 11);
        assert_eq!(ledger.ns[2], 4);
        assert_eq!(ledger.ns.iter().sum::<u64>(), 23);
    }
}
