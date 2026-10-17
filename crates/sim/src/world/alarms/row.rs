//! The alarm row a database's content implies.

use super::*;

/// What a write does to its cell's alarm row.
#[derive(Clone, Copy, Debug)]
pub(in crate::world) enum AlarmOp {
    /// `setAlarm`: install or replace the row, with fresh retry counters.
    Set(i64),
    /// `deleteAlarm`.
    Clear,
    /// A handler that returned consumes the alarm it ran, unless something
    /// replaced that installation meanwhile (the handler generation check).
    Consume { armed_by: WriteId },
    /// A handler that failed reschedules the alarm it ran, on the same
    /// condition, by `celld_logic::alarm::alarm_retry`.
    Retry { armed_by: WriteId, now_ms: i64 },
}

#[derive(Clone, Copy, Debug, Default)]
pub(in crate::world) struct AlarmRow {
    pub at_ms: Option<i64>,
    retry: i64,
    counted: i64,
    /// `_cf_WAKE.sequence`: one step for every change to the row.
    pub sequence: u64,
    /// The write that installed the standing alarm. An alarm keeps it
    /// across restores and epochs, so it names one installation fleet-wide.
    pub armed_by: Option<WriteId>,
}

/// What each write does to its cell's alarm. Folding a database's content
/// through it yields the alarm row that content implies.
#[derive(Default)]
pub(in crate::world) struct AlarmLedger(BTreeMap<WriteId, AlarmOp>);

impl AlarmRow {
    fn apply(&mut self, write: WriteId, op: AlarmOp) {
        let installed =
            |row: &Self, armed_by| row.at_ms.is_some() && row.armed_by == Some(armed_by);
        match op {
            AlarmOp::Set(at_ms) => {
                *self = Self {
                    at_ms: Some(at_ms),
                    retry: 0,
                    counted: 0,
                    sequence: self.sequence + 1,
                    armed_by: Some(write),
                };
            }
            AlarmOp::Clear if self.at_ms.is_some() => self.clear(),
            AlarmOp::Consume { armed_by } if installed(self, armed_by) => self.clear(),
            AlarmOp::Retry { armed_by, now_ms } if installed(self, armed_by) => {
                match celld_logic::alarm::alarm_retry(now_ms, self.retry, self.counted, true) {
                    None => self.clear(),
                    Some(at_ms) => {
                        self.at_ms = Some(at_ms);
                        self.retry += 1;
                        self.counted += 1;
                        self.sequence += 1;
                        self.armed_by = Some(write);
                    }
                }
            }
            _ => {}
        }
    }

    fn clear(&mut self) {
        *self = Self {
            sequence: self.sequence + 1,
            ..Self::default()
        };
    }
}

impl AlarmLedger {
    pub(super) fn record(&mut self, write: WriteId, op: AlarmOp) {
        self.0.insert(write, op);
    }

    /// Whether `write` touched its cell's alarm.
    pub(in crate::world) fn carries(&self, write: WriteId) -> bool {
        self.0.contains_key(&write)
    }

    pub(in crate::world) fn row(&self, content: &[WriteId]) -> AlarmRow {
        let mut row = AlarmRow::default();
        for write in content {
            if let Some(op) = self.0.get(write) {
                row.apply(*write, *op);
            }
        }
        row
    }
}
