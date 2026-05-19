# 02 — Compile the CLI (one-off task)

The `dimster` CLI is a Rust binary you build once from source. Everything else in these docs (`init`, `build`, `deploy`, `run`, `teardown` …) is a
subcommand of this single binary.

## Build it

From the project root:

```bash
cd cli && cargo build --release && cp target/release/dimster .. && cd ..
```

What that does:

1. `cargo build --release` compiles the CLI with optimizations enabled. Expect ~1–2 minutes the first time (Rust downloads and compiles a few
   dozen dependencies); subsequent rebuilds are seconds.
2. `cp target/release/dimster ..` puts the resulting binary at the project root so every example in these docs can call it as `./dimster`.

Expected tail of the build output:
```
   Compiling dimster v0.1.0 (/.../dimster/cli)
    Finished `release` profile [optimized] target(s) in 1m 32s
```

## macOS Apple Silicon: ad-hoc codesign

On Apple Silicon Macs, macOS Gatekeeper refuses to run unsigned binaries copied around the filesystem. After every clean rebuild, sign the binary in
place:

```bash
codesign --force --sign - ./dimster
```

The `-` argument means "ad-hoc sign" (no Apple Developer certificate needed). You'll only need to re-run this after a fresh `cargo build`.

## Verify

Ask the binary to print its help. If it lists a stack of subcommands, you're done:

```bash
./dimster --help
```

You should see something like:
```
Dimster - DIMensional TeSTER for Apache Kafka (or compatible)

Usage: dimster <COMMAND>

Commands:
  build           Build the project and Docker image
  deploy          Deploy Kafka + workers to Kubernetes
  run             Run a benchmark
  explore         Explore performance limits of the Kafka cluster
  correctness     Run a correctness check
  drain-backlog   Measure backlog drain time
  ...
  init            Initialize dimster-config.yaml with a starter set of environments
```

## If something looks wrong

- `command not found: cargo` — Rust isn't on your `PATH`. On macOS run `source "$HOME/.cargo/env"` (or open a new shell). On Linux make sure
  `~/.cargo/bin` is on `PATH`.
- `cannot execute binary file: Exec format error` after copying — the binary was built for the wrong CPU architecture (e.g. ran `cargo build` inside a
  Linux container and copied the result onto a macOS host). Rebuild on the target machine.
- macOS shows a "killed" message or Gatekeeper popup — re-run the `codesign` command above.
