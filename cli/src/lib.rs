#![allow(deprecated)]
use utils::error::Result;
use clap::{AppSettings, Parser, Subcommand};
use core::commands::{self, Formats};
use std::process::exit;

#[derive(Parser, Debug)]
#[clap(name = "taosdump", author, about, long_about = "taosdump CLI", version)]
#[clap(setting = AppSettings::SubcommandRequired)]
#[clap(global_setting(AppSettings::DeriveDisplayOrder))]

pub struct Cli {
    /// Action
    #[clap(subcommand)]
    command: Commands,

    /// Data Format
    format: String,

    /// Set target directory path
    pub path: String,

    /// Number of threads
    pub thread: Option<u32>,
}

#[derive(Subcommand, Debug)]
enum Commands {
    #[clap(
        name = "dumpin",
        about = "dumpin to tdengine database",
        long_about = None, 
    )]
    DumpIn,
    #[clap(
        name = "dumpout",
        about = "dumpout to parquet files",
        long_about = None, 
    )]
    DumpOut,
}


pub fn cli_match() -> Result<()> {
    // Parse the command line arguments
    let cli = Cli::parse();

    let format = match cli.format.to_lowercase().as_str() {
        "avro" => Formats::Avro,
        "parquet" => Formats::Parquet,
        _ => {
            eprintln!("unknown format");
            exit(1)
        }
    };

    // Execute the subcommand
    match &cli.command {
        Commands::DumpIn => commands::dumpin(cli.path.as_str(), cli.thread.unwrap_or(1), format)?,
        Commands::DumpOut => commands::dumpout(cli.path.as_str(), cli.thread.unwrap_or(1), format)?,
    }

    Ok(())
}