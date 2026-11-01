//! The seeds that are known to fail, and exactly how.
//!
//! The core on `main` has bugs this simulator finds and that are not fixed
//! yet (`docs/testing.md`). Excusing them by kind would also excuse a new bug
//! that happens to fail the same way. So `known-failures.txt` lists each
//! failing seed with everything it fails with: the kinds of the properties
//! it breaks, and the `State::validate` message it stops on, if any. A run
//! is known only if it fails exactly as listed. A seed that fails otherwise
//! is new, and a listed seed that fails less, or passes, is stale: either
//! way the list must change, so every change to a run is seen.
//!
//! One line per seed; a line starting with `#` is a comment:
//!
//! ```text
//! SEED CONFIG MODE [KIND ...] [panic: MESSAGE]
//! 39 swarm validated liveness
//! 1 swarm validated panic: confirming cell "c3" left the owner read in Inactive
//! ```
//!
//! `CONFIG` names the configuration the seed ran in: `swarm` is
//! `SimConfig::swarm(seed)`, and the test suite names its own. `MODE` is
//! `validated` when `State::validate` runs after every event (a build with
//! debug assertions) and `unvalidated` when it is compiled out, as in
//! production: the two modes fail differently.

use std::collections::{BTreeMap, BTreeSet};

use crate::Outcome;

/// Whether this build runs `State::validate`: the core checks it only with
/// debug assertions on, and both crates build in the same profile.
pub const MODE: &str = if cfg!(debug_assertions) {
    "validated"
} else {
    "unvalidated"
};

/// Everything one run failed with.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Failure {
    pub kinds: BTreeSet<String>,
    pub panic: Option<String>,
}

impl Failure {
    /// What `outcome` failed with, or `None` if it passed.
    pub fn of(outcome: &Outcome) -> Option<Self> {
        if outcome.passed() {
            return None;
        }
        Some(Self {
            kinds: outcome.violations.iter().map(|v| v.kind.into()).collect(),
            panic: outcome.panic.as_deref().map(panic_message),
        })
    }

    /// The list line for this failure.
    pub fn line(&self, seed: u64, config: &str) -> String {
        let mut line = format!("{seed} {config} {MODE}");
        for kind in &self.kinds {
            line += &format!(" {kind}");
        }
        if let Some(panic) = &self.panic {
            line += &format!(" panic: {panic}");
        }
        line
    }
}

/// A panic as `World::run` reports it, without its seed and time, and an
/// invariant without the quoting `expect` adds: `seed 1 t=26964: state
/// invariant: "cell \"c3\" …"` becomes `cell "c3" …`.
fn panic_message(panic: &str) -> String {
    let message = panic.split_once(": ").map_or(panic, |(_, rest)| rest);
    match message
        .strip_prefix("state invariant: \"")
        .and_then(|m| m.strip_suffix('"'))
    {
        Some(invariant) => invariant.replace("\\\"", "\""),
        None => message.to_string(),
    }
}

/// How a run compares with the list.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Verdict {
    Passed,
    /// It failed exactly as listed.
    Known,
    /// It failed, and not as listed: the list names `listed`.
    New {
        listed: Option<Failure>,
    },
    /// It is listed, and passed.
    Stale {
        listed: Failure,
    },
}

impl Verdict {
    /// The run passed, or failed exactly as listed.
    pub fn ok(&self) -> bool {
        matches!(self, Verdict::Passed | Verdict::Known)
    }
}

/// The parsed list, for this build's mode.
#[derive(Debug, Default)]
pub struct KnownFailures {
    listed: BTreeMap<(u64, String), Failure>,
}

impl KnownFailures {
    /// Parse `text`, keeping the lines for this build's mode.
    pub fn parse(text: &str) -> Result<Self, String> {
        let mut known = Self::default();
        for (number, line) in text.lines().enumerate() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let bad = |why: &str| format!("line {}: {why}: {line:?}", number + 1);
            let (head, panic) = match line.split_once(" panic: ") {
                Some((head, panic)) => (head, Some(panic.trim().to_string())),
                None => (line, None),
            };
            let mut fields = head.split_whitespace();
            let seed = fields.next().and_then(|s| s.parse().ok());
            let seed = seed.ok_or_else(|| bad("no seed"))?;
            let config = fields.next().ok_or_else(|| bad("no config"))?;
            let mode = fields.next().ok_or_else(|| bad("no mode"))?;
            if !["validated", "unvalidated"].contains(&mode) {
                return Err(bad("mode is neither validated nor unvalidated"));
            }
            let failure = Failure {
                kinds: fields.map(str::to_string).collect(),
                panic,
            };
            if failure == Failure::default() {
                return Err(bad("lists no failure"));
            }
            if mode != MODE {
                continue;
            }
            let key = (seed, config.to_string());
            if known.listed.insert(key, failure).is_some() {
                return Err(bad("lists the seed twice"));
            }
        }
        Ok(known)
    }

    /// Every failure the list names for this build's mode.
    pub fn failures(&self) -> impl Iterator<Item = &Failure> {
        self.listed.values()
    }

    pub fn judge(&self, seed: u64, config: &str, outcome: &Outcome) -> Verdict {
        let listed = self.listed.get(&(seed, config.to_string())).cloned();
        match (Failure::of(outcome), listed) {
            (None, None) => Verdict::Passed,
            (None, Some(listed)) => Verdict::Stale { listed },
            (Some(failure), listed) if listed.as_ref() == Some(&failure) => Verdict::Known,
            (Some(_), listed) => Verdict::New { listed },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{run, SimConfig, Violation};

    fn failing(kinds: &[&'static str], panic: Option<&str>) -> Outcome {
        let mut outcome = run(0, &SimConfig::calm());
        outcome.panic = panic.map(str::to_string);
        outcome.violations = (kinds.iter())
            .map(|kind| Violation {
                at_ms: 0,
                kind,
                detail: String::new(),
            })
            .collect();
        outcome
    }

    #[test]
    fn a_run_is_known_only_if_it_fails_exactly_as_listed() {
        let list = format!(
            "# a comment\n\
             7 swarm {MODE} liveness stale-read\n\
             9 chaos {MODE} panic: cell \"c3\" is gone\n"
        );
        let known = KnownFailures::parse(&list).unwrap();
        let as_listed = failing(&["stale-read", "liveness", "liveness"], None);
        assert_eq!(known.judge(7, "swarm", &as_listed), Verdict::Known);
        // Another seed or configuration is new.
        assert!(!known.judge(8, "swarm", &as_listed).ok());
        assert!(!known.judge(7, "chaos", &as_listed).ok());
        // So is a failure with more in it, or less.
        let more = failing(&["stale-read", "liveness", "lost-write"], None);
        assert!(!known.judge(7, "swarm", &more).ok());
        assert!(!known.judge(7, "swarm", &failing(&["liveness"], None)).ok());
        // A listed seed that passes is stale.
        assert!(!known.judge(7, "swarm", &failing(&[], None)).ok());
        // A panic must match its message, after the seed, time and quoting.
        let gone = r#"seed 9 t=5: state invariant: "cell \"c3\" is gone""#;
        assert!(known.judge(9, "chaos", &failing(&[], Some(gone))).ok());
        let other = r#"seed 9 t=5: state invariant: "cell \"c4\" is gone""#;
        assert!(!known.judge(9, "chaos", &failing(&[], Some(other))).ok());
        // And the line a failure prints parses back to the same failure.
        let line = Failure::of(&more).unwrap().line(7, "swarm");
        let reparsed = KnownFailures::parse(&line).unwrap();
        assert_eq!(reparsed.judge(7, "swarm", &more), Verdict::Known);
    }
}
