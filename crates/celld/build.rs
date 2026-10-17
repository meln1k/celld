// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! Points the harness slots at the in-tree harness.
//!
//! A build with `--cfg celld_internal_tests` includes harness files through
//! `include!(env!("CELLD_INTERNAL_…"))` and `include!(env!("CELLD_CONFORMANCE_…"))`.
//! Each slot reads `internal/<name>.rs` or `conformance/<name>.rs`, where
//! `<name>` is the slot's suffix in lower case, and an empty file when that
//! does not exist. A slot already set in the environment is left alone, so
//! an external harness still takes precedence. Other builds skip all of it.

// The build script runs on the build host, outside the execution boundary.
#![allow(clippy::disallowed_methods)]

use std::io::Write as _;
use std::path::{Path, PathBuf};

/// One `cargo:` directive. `println!` is forbidden crate-wide.
macro_rules! cargo {
    ($($arg:tt)*) => {
        writeln!(std::io::stdout(), $($arg)*).expect("write a cargo directive")
    };
}

fn main() {
    cargo!("cargo:rerun-if-changed=build.rs");
    if std::env::var_os("CARGO_CFG_CELLD_INTERNAL_TESTS").is_none() {
        return;
    }
    let root = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").unwrap());
    // The slots come from the sources and the files from the harness
    // directories, so any change in the crate can move one.
    cargo!("cargo:rerun-if-changed=.");
    let empty = root.join("internal/empty.rs");
    let mut slots = Vec::new();
    collect_slots(&root, &mut slots);
    slots.sort();
    slots.dedup();
    for slot in slots {
        cargo!("cargo:rerun-if-env-changed={slot}");
        if std::env::var_os(&slot).is_some() {
            continue;
        }
        let (dir, suffix) = match slot.strip_prefix("CELLD_INTERNAL_") {
            Some(suffix) => ("internal", suffix),
            None => ("conformance", &slot["CELLD_CONFORMANCE_".len()..]),
        };
        let file = root.join(dir).join(format!("{}.rs", suffix.to_lowercase()));
        let path = if file.exists() { file } else { empty.clone() };
        cargo!("cargo:rustc-env={slot}={}", path.display());
    }
}

/// Every `env!("CELLD_INTERNAL_…")` or `env!("CELLD_CONFORMANCE_…")` named in
/// the crate's sources, outside the harness directories.
fn collect_slots(dir: &Path, slots: &mut Vec<String>) {
    for entry in std::fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        let name = path.file_name().unwrap().to_string_lossy();
        if path.is_dir() {
            if !matches!(&*name, "internal" | "conformance" | "target") {
                collect_slots(&path, slots);
            }
            continue;
        }
        if !name.ends_with(".rs") {
            continue;
        }
        let source = std::fs::read_to_string(&path).unwrap();
        for prefix in ["env!(\"CELLD_INTERNAL_", "env!(\"CELLD_CONFORMANCE_"] {
            for (start, _) in source.match_indices(prefix) {
                let rest = &source[start + "env!(\"".len()..];
                if let Some(end) = rest.find('"') {
                    slots.push(rest[..end].to_string());
                }
            }
        }
    }
}
