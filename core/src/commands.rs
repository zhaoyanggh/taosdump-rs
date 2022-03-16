use std::path::PathBuf;

use super::dumpin;
use super::dumpout;
use utils::error::Result;
use utils::get_all_data_files;
pub use utils::Formats;

pub fn dumpin(path: &str, thread: u32, format: Formats) -> Result<()> {
    let mut file_list: Vec<PathBuf> = vec![];
    get_all_data_files(path, &mut file_list, &format);
    dumpin::start(&file_list, thread, format)?;
    Ok(())
}

pub fn dumpout(path: &str, thread: u32, format: Formats, name: String) -> Result<()> {
    dumpout::dumpout_database_sql(path, name.clone())?;
    dumpout::dumpout_stable_sql(path, name.clone())?;
    dumpout::start(path, thread, format)?;
    Ok(())
}
