use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand};

use objex::config::{self, Config};

#[derive(Parser)]
#[command(name = "objex", version, about = "S3 / R2 compatible object storage server")]
struct Cli {
    /// Config file
    #[arg(long, short, global = true, default_value = "objex.toml", env = "OBJEX_CONFIG")]
    config: PathBuf,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Write a default config file
    Init,
    /// Run the S3 server
    Server {
        /// Data directory (overrides the config file)
        #[arg(long)]
        data: Option<PathBuf>,
        /// Listen address (overrides the config file)
        #[arg(long)]
        listen: Option<String>,
        /// Region reported to clients
        #[arg(long)]
        region: Option<String>,
        /// Base domain for virtual-host style requests
        #[arg(long)]
        domain: Option<String>,
        /// Acknowledge writes without fsync (faster, not crash-safe)
        #[arg(long)]
        no_fsync: bool,
    },
    /// Manage access keys
    Key {
        #[command(subcommand)]
        command: KeyCommand,
    },
}

#[derive(Subcommand)]
enum KeyCommand {
    /// Generate a new key pair
    Add {
        name: String,
        /// Restrict the key to these buckets (repeatable)
        #[arg(long = "bucket")]
        buckets: Vec<String>,
        /// Only allow reads (GET, HEAD, list)
        #[arg(long)]
        read_only: bool,
    },
    /// List keys
    List,
    /// Remove a key by access key or name
    Rm { key: String },
}

fn run(cli: Cli) -> Result<(), String> {
    match cli.command {
        Command::Init => {
            config::init_config(&cli.config)?;
            println!("wrote {}", cli.config.display());
            Ok(())
        }
        Command::Server { data, listen, region, domain, no_fsync } => {
            let mut cfg = Config::load(&cli.config)?;
            if let Some(d) = data {
                cfg.data_dir = d;
            }
            if let Some(l) = listen {
                cfg.listen = l;
            }
            if let Some(r) = region {
                cfg.region = r;
            }
            if let Some(d) = domain {
                cfg.domain = d;
            }
            if no_fsync {
                cfg.fsync = false;
            }
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .map_err(|e| e.to_string())?
                .block_on(objex::server::run(cfg, cli.config))
        }
        Command::Key { command } => match command {
            KeyCommand::Add { name, buckets, read_only } => {
                let k = config::add_key(&cli.config, &name, buckets, read_only)?;
                println!("added key \"{}\" to {}", k.name, cli.config.display());
                println!("access key: {}", k.access_key);
                println!("secret key: {}", k.secret_key);
                Ok(())
            }
            KeyCommand::List => {
                let cfg = Config::load(&cli.config)?;
                if cfg.keys.is_empty() {
                    println!("no keys");
                }
                for k in cfg.keys {
                    let buckets = if k.buckets.is_empty() { "all buckets".to_string() } else { k.buckets.join(",") };
                    println!("{:<24} {:<22} {}{}", k.name, k.access_key, buckets, if k.read_only { " (read-only)" } else { "" });
                }
                Ok(())
            }
            KeyCommand::Rm { key } => match config::remove_key(&cli.config, &key)? {
                0 => Err(format!("no key named or with access key {key}")),
                n => {
                    println!("removed {n} key(s)");
                    Ok(())
                }
            },
        },
    }
}

fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::try_from_env("OBJEX_LOG").unwrap_or_else(|_| "info".into()))
        .init();
    match run(Cli::parse()) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::FAILURE
        }
    }
}
