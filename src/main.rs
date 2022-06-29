use clap::Parser;
use solana_snapshot_etl::UnpackedSnapshotLoader;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    path: String,
}

fn main() {
    env_logger::init();
    if let Err(e) = _main() {
        eprintln!("{}", e);
        std::process::exit(1);
    }
}

fn _main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let loader = UnpackedSnapshotLoader::open(&args.path)?;
    loader.foo();
    Ok(())
}
