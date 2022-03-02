#![allow(deprecated)]
use std::path::PathBuf;
use utils::error::Result;
use clap::{AppSettings, Parser, Subcommand};
use core::commands;
#[derive(Parser, Debug)]
#[clap(name = "taosdump", author, about, long_about = "taosdump CLI", version)]
#[clap(setting = AppSettings::SubcommandRequired)]
#[clap(global_setting(AppSettings::DeriveDisplayOrder))]
pub struct Cli {
    /// Set target directory path
    pub path: Option<PathBuf>,

    /// Action
    #[clap(subcommand)]
    command: Commands,
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

    // Execute the subcommand
    match &cli.command {
        Commands::DumpIn => commands::dumpin()?,
        Commands::DumpOut => commands::dumpout()?,
    }

    Ok(())
}