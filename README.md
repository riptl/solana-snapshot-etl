# Solana Snapshot ETL 📸

<sub>Built with 🦀 at <em>REDACTED</em></sub>

[![crates.io](https://img.shields.io/crates/v/solana-snapshot-etl?style=flat-square&logo=rust&color=blue)](https://crates.io/crates/solana-snapshot-etl)
[![docs.rs](https://img.shields.io/badge/docs.rs-solana--snapshot--etl-blue?style=flat-square&logo=docs.rs)](https://docs.rs/solana-snapshot-etl)
[![license](https://img.shields.io/badge/license-Apache--2.0-blue?style=flat-square)](#license)

**`solana-snapshot-etl` efficiently extracts all accounts in a snapshot** to load them into an external system.

## Motivation

Solana nodes periodically backup their account database into a `.tar.zst` "snapshot" stream.
If you run a node yourself, you've probably seen a snapshot file such as this one already:

```
snapshot-139240745-D17vR2iksG5RoLMfTX7i5NwSsr4VpbybuX1eqzesQfu2.tar.zst
```

A full snapshot file contains a copy of all accounts at a specific slot state (in this case slot `139240745`).

Historical accounts data is relevant to blockchain analytics use-cases and event tracing.
Despite archives being readily available, the ecosystem was missing an easy-to-use tool to access snapshot data.

## Usage

The standalone command-line tool can export data to CSV, SQLite3 and Geyser plugins.

Build from source.

```shell
cargo build --release --bin solana-snapshot-etl --features-standalone
```

**Dump all token accounts to SQLite.**

```shell
./target/release/solana-snapshot-etl snapshot-139240745-*.tar.zst --sqlite-out snapshot.db
```

**Replicate accounts to a Geyser plugin.**

```shell
./target/release/solana-snapshot-etl snapshot-139240745-*.tar.zst --geyser plugin-config.json
```
