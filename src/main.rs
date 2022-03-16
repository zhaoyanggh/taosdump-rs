use utils::error::Result;
extern crate log;
extern crate pretty_env_logger;
/// The main entry point of the application.
fn main() -> Result<()> {
    pretty_env_logger::init();
    // Match Commands
    cli::cli_match()?;

    Ok(())
}
