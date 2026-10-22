//! The seeds a sweep expects to fail, and how.
//!
//! An open finding fails its seeds on every sweep until the core is fixed,
//! and a sweep that always fails cannot tell a new regression from the old
//! findings. `known-failures.txt` lists each expected failure as a seed, the
//! posture it fails in, and what it fails with. A sweep then fails on a seed
//! that fails in a way the list does not name, and on a listed seed that no
//! longer fails, so the list shrinks as the findings are fixed.
//!
//! One expectation per line, after `#` comments and blank lines:
//!
//! ```text
//! 259 swarm lost-write
//! 25 swarm panic: exceeds release ceiling
//! ```
//!
//! The posture is `swarm` (as `SimConfig::swarm` draws it), `fleet`, or
//! `bucket` (`--fleet on` and `--fleet off`). An expectation is a checker's
//! kind, or `panic:` and text the panic message contains. A seed may have
//! several lines; its failure is known when every violation kind and its
//! panic each match one of them.

use crate::Outcome;
use std::collections::BTreeMap;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Posture {
    Swarm,
    Fleet,
    Bucket,
}

impl Posture {
    /// The posture a `--fleet` setting forces, if any.
    pub fn of(fleet: Option<bool>) -> Self {
        match fleet {
            None => Posture::Swarm,
            Some(true) => Posture::Fleet,
            Some(false) => Posture::Bucket,
        }
    }

    fn parse(text: &str) -> Option<Self> {
        match text {
            "swarm" => Some(Posture::Swarm),
            "fleet" => Some(Posture::Fleet),
            "bucket" => Some(Posture::Bucket),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum Expect {
    Violation(String),
    Panic(String),
}

/// How a seed's outcome compares with the list.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Verdict {
    Passed,
    /// It failed as listed.
    Known,
    /// It failed, and the list does not say so.
    Failed,
    /// It is listed, and passed.
    Stale,
}

#[derive(Debug, Default)]
pub struct KnownFailures {
    expected: BTreeMap<(u64, Posture), Vec<Expect>>,
}

impl KnownFailures {
    pub fn parse(text: &str) -> Result<Self, String> {
        let mut known = KnownFailures::default();
        for (number, line) in text.lines().enumerate() {
            let line = line.split('#').next().unwrap_or("").trim();
            if line.is_empty() {
                continue;
            }
            let bad = || format!("line {}: {line:?}", number + 1);
            let mut fields = line.splitn(3, char::is_whitespace);
            let seed = fields.next().and_then(|s| s.parse().ok()).ok_or_else(bad)?;
            let posture = fields.next().and_then(Posture::parse).ok_or_else(bad)?;
            let expect = fields.next().map(str::trim).ok_or_else(bad)?;
            let expect = match expect.strip_prefix("panic:") {
                Some(text) if !text.trim().is_empty() => Expect::Panic(text.trim().to_string()),
                Some(_) => return Err(bad()),
                None if !expect.contains(char::is_whitespace) => {
                    Expect::Violation(expect.to_string())
                }
                None => return Err(bad()),
            };
            known
                .expected
                .entry((seed, posture))
                .or_default()
                .push(expect);
        }
        Ok(known)
    }

    pub fn judge(&self, seed: u64, posture: Posture, outcome: &Outcome) -> Verdict {
        let Some(expected) = self.expected.get(&(seed, posture)) else {
            return if outcome.failed() {
                Verdict::Failed
            } else {
                Verdict::Passed
            };
        };
        if !outcome.failed() {
            return Verdict::Stale;
        }
        let panic_known = outcome.panic.as_ref().is_none_or(|panic| {
            expected.iter().any(
                |expect| matches!(expect, Expect::Panic(text) if panic.contains(text.as_str())),
            )
        });
        let violations_known = outcome.violations.iter().all(|violation| {
            expected
                .iter()
                .any(|expect| matches!(expect, Expect::Violation(kind) if kind == violation.kind))
        });
        if panic_known && violations_known {
            Verdict::Known
        } else {
            Verdict::Failed
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Coverage, Stats, Violation};

    fn outcome(panic: Option<&str>, kinds: &[&'static str]) -> Outcome {
        Outcome {
            seed: 0,
            violations: kinds
                .iter()
                .map(|kind| Violation {
                    at_ms: 0,
                    kind,
                    detail: String::new(),
                })
                .collect(),
            stats: Stats::default(),
            coverage: Coverage::default(),
            trace_hash: 0,
            trace: Vec::new(),
            panic: panic.map(str::to_string),
        }
    }

    const LIST: &str = "
        # a comment
        7 swarm lost-write   # trailing comment
        7 swarm alarm-refired
        9 fleet panic: exceeds release ceiling
    ";

    #[test]
    fn a_failure_is_known_only_as_listed() {
        let known = KnownFailures::parse(LIST).unwrap();
        let swarm = Posture::Swarm;
        let lost = outcome(None, &["lost-write", "alarm-refired"]);
        assert_eq!(known.judge(7, swarm, &lost), Verdict::Known);
        // Another posture, another seed, or another way to fail is new.
        assert_eq!(known.judge(7, Posture::Fleet, &lost), Verdict::Failed);
        assert_eq!(known.judge(8, swarm, &lost), Verdict::Failed);
        let forked = outcome(None, &["lost-write", "two-writers"]);
        assert_eq!(known.judge(7, swarm, &forked), Verdict::Failed);
        let panicked = outcome(Some("t=1 p1: state invariant: x"), &["lost-write"]);
        assert_eq!(known.judge(7, swarm, &panicked), Verdict::Failed);
        // A listed seed that passes is stale.
        assert_eq!(known.judge(7, swarm, &outcome(None, &[])), Verdict::Stale);
        assert_eq!(known.judge(8, swarm, &outcome(None, &[])), Verdict::Passed);

        let ceiling = "t=1 p1: state invariant: \"handoffs 3 exceeds release ceiling 2\"";
        let fleet = Posture::Fleet;
        assert_eq!(
            known.judge(9, fleet, &outcome(Some(ceiling), &[])),
            Verdict::Known
        );
        let other = "t=1 p1: state invariant: \"occupied counter 6 disagrees\"";
        assert_eq!(
            known.judge(9, fleet, &outcome(Some(other), &[])),
            Verdict::Failed
        );
    }

    #[test]
    fn a_malformed_line_is_refused() {
        for line in [
            "7",
            "7 swarm",
            "x swarm lost-write",
            "7 pond lost-write",
            "7 swarm panic:",
        ] {
            assert!(KnownFailures::parse(line).is_err(), "{line}");
        }
    }
}
