<div align="center">
  <h1>Solana Snapshot ETL 📸</h1>
  <p>
    <strong>Rust tool to efficiently unpack Solana snapshots</strong>
  </p>
  <sub>Built with 🦀 at <em>REDACTED</em></sub>
</div>

## Motivation

Solana nodes periodically backup their account database into a `.tar.zst` "snapshot" stream.
If you run a node yourself, you've probably seen a snapshot file such as this one already: 

```
snapshot-139240745-D17vR2iksG5RoLMfTX7i5NwSsr4VpbybuX1eqzesQfu2.tar.zst
```

A full snapshot file contains a copy of all accounts at a specific slot state (in this case slot `139240745`).

Historical accounts data is relevant to blockchain analytics use-cases and event tracing.
Despite archives being readily available, the ecosystem lacks an easy-to-use tool to access snapshot data.

**`solana-snapshot-etl` efficiently extracts all accounts in a snapshot** to load them into an external system.

## Usage

### As a command-line tool

Build from source.

```shell
cargo build --release --bin solana-snapshot-etl --features-standalone
```

Unpack a snapshot.

```shell
mkdir ./unpacked_snapshot
cd ./unpacked_snapshot
tar -I zstd -xvf ../snapshot-139240745-D17vR2iksG5RoLMfTX7i5NwSsr4VpbybuX1eqzesQfu2.tar.zst
```

Dump all token accounts to SQLite.

```shell
cd ../
./target/release/solana-snapshot-etl ./unpacked_snapshot --sqlite-out snapshot.db
```

### As a library

TODO
