//! Rule/call-site attribution on the existing exclusive ledger, never a
//! separate clock for the partition itself. Inclusive refresh measurements
//! are diagnostic cross-checks and must not be added to exclusive buckets.
use super::*;
use std::collections::BTreeMap;

const SUBS: usize = 9; // apply residual followed by B3a..h

#[derive(Clone, Copy, Default)]
#[repr(usize)]
pub(crate) enum CacheSite {
    #[default]
    Other,
    Owned,
    Native,
}

#[derive(Clone, Copy)]
#[repr(usize)]
pub(crate) enum MissKind {
    NewContent,
    SameContentDifferentKey,
    SameKeyUnresident,
    Unverified,
}

#[derive(Default)]
pub(super) struct RuleRow {
    /// Apply residual, then B3a..h; exclusive and additive.
    ns: [u64; SUBS],
    entries: [u64; SUBS],
    /// Call sites: other / owned / native. Miss classes follow MissKind.
    local_hits: [u64; 3],
    local_misses: [[u64; 4]; 3],
    native_refresh_calls: u64,
    native_refresh_requested_nodes: u64,
    native_refresh_visited_nodes: u64,
    native_refresh_inclusive_ns: u64,
    native_refresh_size_histogram: BTreeMap<usize, u64>,
}

#[derive(Default)]
pub(super) struct Attribution {
    rule: Option<u32>,
    site: CacheSite,
    pub(super) rows: BTreeMap<u32, RuleRow>,
}

fn sub(bucket: usize) -> Option<usize> {
    if bucket == Bucket::Apply as usize {
        Some(0)
    } else if (Bucket::NativeConstruct as usize..=Bucket::Encoding as usize).contains(&bucket) {
        Some(bucket - Bucket::NativeConstruct as usize + 1)
    } else {
        None
    }
}

impl Attribution {
    pub(super) fn active(&self) -> bool {
        self.rule.is_some()
    }
    pub(super) fn json(&self) -> serde_json::Value {
        serde_json::Value::Object(
            self.rows
                .iter()
                .map(|(rule, row)| {
                    (
                        rule.to_string(),
                        serde_json::json!({"ns": row.ns, "entries": row.entries,
                            "local_hits": row.local_hits, "local_misses": row.local_misses,
                            "native_refresh_calls": row.native_refresh_calls,
                            "native_refresh_requested_nodes": row.native_refresh_requested_nodes,
                            "native_refresh_visited_nodes": row.native_refresh_visited_nodes,
                            "native_refresh_inclusive_ns": row.native_refresh_inclusive_ns,
                            "native_refresh_size_histogram": row.native_refresh_size_histogram,
                        }),
                    )
                })
                .collect(),
        )
    }
    pub(super) fn account(&mut self, bucket: usize, ns: u64) {
        if let Some(index) = sub(bucket) {
            self.rows.entry(self.rule.unwrap_or(0)).or_default().ns[index] += ns;
        }
    }
    pub(super) fn entered(&mut self, bucket: usize) {
        if let Some(index) = sub(bucket) {
            self.rows.entry(self.rule.unwrap_or(0)).or_default().entries[index] += 1;
        }
    }
}

pub(crate) struct AttributionScope {
    previous: Option<(u64, Option<u32>, CacheSite)>,
    _not_send: PhantomData<Rc<()>>,
}

fn attribution(rule: Option<u32>, site: Option<CacheSite>) -> AttributionScope {
    let previous = SLOT.with(|slot| {
        let mut slot = slot.borrow_mut();
        let generation = slot.generation;
        let ledger = slot.ledger.as_mut()?;
        ledger.change(ledger.now(), ledger.current);
        let previous = (generation, ledger.b3.rule, ledger.b3.site);
        if let Some(rule) = rule {
            ledger.b3.rule = Some(rule);
        }
        if let Some(site) = site {
            ledger.b3.site = site;
        }
        Some(previous)
    });
    AttributionScope {
        previous,
        _not_send: PhantomData,
    }
}
pub(crate) fn rule(rule: u32) -> AttributionScope {
    attribution(Some(rule), None)
}
pub(crate) fn cache_site(site: CacheSite) -> AttributionScope {
    attribution(None, Some(site))
}

impl Drop for AttributionScope {
    fn drop(&mut self) {
        if let Some((generation, rule, site)) = self.previous {
            SLOT.with(|slot| {
                let mut slot = slot.borrow_mut();
                if slot.generation == generation {
                    if let Some(ledger) = slot.ledger.as_mut() {
                        ledger.change(ledger.now(), ledger.current);
                        ledger.b3.rule = rule;
                        ledger.b3.site = site;
                    }
                }
            });
        }
    }
}

pub(crate) fn local_lookup(miss: Option<MissKind>) {
    SLOT.with(|slot| {
        let mut slot = slot.borrow_mut();
        if let Some(ledger) = slot.ledger.as_mut() {
            let b3 = &mut ledger.b3;
            let row = b3.rows.entry(b3.rule.unwrap_or(0)).or_default();
            match miss {
                None => row.local_hits[b3.site as usize] += 1,
                Some(kind) => row.local_misses[b3.site as usize][kind as usize] += 1,
            }
        }
    });
}

pub(crate) struct RefreshScope {
    start: Option<(u64, u64, u32)>,
    _not_send: PhantomData<Rc<()>>,
}
pub(crate) fn native_refresh(nodes: usize) -> RefreshScope {
    let start = SLOT.with(|slot| {
        let mut slot = slot.borrow_mut();
        let generation = slot.generation;
        let ledger = slot.ledger.as_mut()?;
        let now = ledger.now();
        let rule = ledger.b3.rule.unwrap_or(0);
        let row = ledger.b3.rows.entry(rule).or_default();
        row.native_refresh_calls += 1;
        row.native_refresh_requested_nodes += nodes as u64;
        // 64 means >=64; bound diagnostic state independently of shell size.
        *row.native_refresh_size_histogram
            .entry(nodes.min(64))
            .or_default() += 1;
        Some((generation, now, rule))
    });
    RefreshScope {
        start,
        _not_send: PhantomData,
    }
}
pub(crate) fn native_refresh_node() {
    SLOT.with(|slot| {
        if let Some(ledger) = slot.borrow_mut().ledger.as_mut() {
            ledger
                .b3
                .rows
                .entry(ledger.b3.rule.unwrap_or(0))
                .or_default()
                .native_refresh_visited_nodes += 1;
        }
    });
}
impl Drop for RefreshScope {
    fn drop(&mut self) {
        if let Some((generation, start, rule)) = self.start {
            SLOT.with(|slot| {
                let mut slot = slot.borrow_mut();
                if slot.generation == generation {
                    if let Some(ledger) = slot.ledger.as_mut() {
                        let elapsed = ledger.now() - start;
                        ledger
                            .b3
                            .rows
                            .entry(rule)
                            .or_default()
                            .native_refresh_inclusive_ns += elapsed;
                    }
                }
            });
        }
    }
}
