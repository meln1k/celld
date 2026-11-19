// Copyright 2026 Deno Land Inc. Apache-2.0 license.

// The test starts the built binary on a copy of an example and talks to it
// over a plain socket, outside the execution boundary.
#![allow(clippy::disallowed_methods)]

//! A facet call takes turns on an isolate from inside an op. An op is polled
//! only by its event's driver, between that driver's turns, so a facet op's
//! body runs on a task of its own (`asyncrt::enqueue_on_own_task`). The pool
//! refuses a turn inside a driver's op in builds with debug assertions, as
//! this test's binary is: a facet op back on the driver fails every call here,
//! where in production it would stall the isolate only under a rare timing.
//!
//! `celld dev` bundles with esbuild, found on `PATH` or in `CELLD_ESBUILD`:
//!
//! ```sh
//! cargo test -p celld --test facet_turns
//! ```

use std::io::Read;
use std::io::Write;
use std::net::TcpListener;
use std::net::TcpStream;
use std::path::Path;
use std::process::Child;
use std::process::Command;
use std::process::Stdio;
use std::time::Duration;
use std::time::Instant;

struct Server(Child);

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .and_then(|listener| listener.local_addr())
        .expect("a free local port")
        .port()
}

/// One request on a fresh connection; `None` while nothing listens yet.
fn request(port: u16, path: &str) -> Option<String> {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).ok()?;
    stream
        .set_read_timeout(Some(Duration::from_secs(20)))
        .expect("a read timeout");
    stream
        .write_all(
            format!("GET {path} HTTP/1.1\r\nhost: localhost\r\nconnection: close\r\n\r\n")
                .as_bytes(),
        )
        .expect("the request is written");
    let mut response = String::new();
    stream
        .read_to_string(&mut response)
        .unwrap_or_else(|error| panic!("GET {path} got no answer: {error}"));
    Some(response)
}

#[test]
fn a_facet_call_takes_its_turns_off_the_driver() {
    let example = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../examples/facets");
    let project = tempfile::tempdir().expect("a project directory");
    for file in ["index.js", "wrangler.jsonc"] {
        std::fs::copy(example.join(file), project.path().join(file)).expect("the facets example");
    }
    let port = free_port();
    let mut server = Server(
        Command::new(env!("CARGO_BIN_EXE_celld"))
            .arg("dev")
            .arg(project.path())
            .args(["--no-watch", "--port", &port.to_string()])
            .stdout(Stdio::null())
            .spawn()
            .expect("celld starts"),
    );

    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        if let Some(status) = server.0.try_wait().expect("celld's status") {
            panic!("celld exited with {status}; `celld dev` bundles with esbuild, on PATH or in CELLD_ESBUILD");
        }
        if request(port, "/.well-known/celld/health")
            .is_some_and(|response| response.starts_with("HTTP/1.1 200"))
        {
            break;
        }
        assert!(Instant::now() < deadline, "celld did not become ready");
        std::thread::sleep(Duration::from_millis(200));
    }

    // Each call counts once in the facet's own storage.
    for count in 0..3 {
        let response = request(port, "/").expect("celld still listens");
        assert!(
            response.starts_with("HTTP/1.1 200")
                && response.contains(&format!("{count}:hello, facet")),
            "facet call {count} answered {response}"
        );
    }
}
