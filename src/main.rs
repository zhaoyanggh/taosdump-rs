use utils::error::Result;

/// The main entry point of the application.
fn main() -> Result<()> {
    // Match Commands
    cli::cli_match()?;

    Ok(())
}
