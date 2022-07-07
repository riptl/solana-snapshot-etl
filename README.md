# Solana Snapshot ETL 📸

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

## Building

```shell
cargo install --git https://github.com/terorie/solana-snapshot-etl --features=standalone --bins
```

## Usage

The ETL tool can extract snapshots from a variety of streaming sources
and load them into one of the supported storage backends.

The basic command-line usage is as follows:

```
USAGE:
    solana-snapshot-etl [OPTIONS] <LOAD_FLAGS> <SOURCE>
```

### Sources

Extract from a local snapshot file:

```shell
solana-snapshot-etl /path/to/snapshot-*.tar.zst ...
```

Extract from an unpacked snapshot:

```shell
# Example unarchive command
tar -I zstd -xvf snapshot-*.tar.zst ./unpacked_snapshot/

solana-snapshot-etl ./unpacked_snapshot/
```

Stream snapshot from HTTP source or S3 bucket:

```shell
solana-snapshot-etl 'https://my-solana-node.bdnodes.net/snapshot.tar.zst?auth=xxx' ...
```

### Targets

#### SQLite3 (recommended)

The fastest way to access snapshot data is the SQLite3 load mechanism.

The resulting SQLite database file can be loaded using any SQLite client library.

```shell
solana-snapshot-etl snapshot-139240745-*.tar.zst --sqlite-out snapshot.db
```

The resulting SQLite database contains the following tables.

- `account`
- `token_account` (SPL Token Program)
- `token_mint` (SPL Token Program)
- `token_multisig` (SPL Token Program)
- `token_metadata` (MPL Metadata Program)

#### CSV

Coming soon!

#### Geyser plugin

Much like `solana-validator`, this tool can write account updates to Geyser plugins.

```shell
solana-snapshot-etl snapshot-139240745-*.tar.zst --geyser plugin-config.json
```

For more info, consult Solana's docs: https://docs.solana.com/developing/plugins/geyser-plugins

#### Dump programs

The `--programs-out` flag exports all Solana programs (in ELF format).

```shell
solana-snapshot-etl snapshot-139240745-*.tar.zst --programs-out programs.tar
```

or to extract in place

```shell
solana-snapshot-etl snapshot-139240745-*.tar.zst --programs-out - | tar -xv
```
