use super::dumpin;
use super::dumpout;
use utils::error::Result;

pub fn dumpin() -> Result<()> {
    dumpin::start()?;
    Ok(())
}

pub fn dumpout() -> Result<()> {
    dumpout::start()?;
    Ok(())
}
