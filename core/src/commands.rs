use std::path::PathBuf;

use super::dumpin;
use super::dumpout;
use utils::error::Result;
use utils::get_all_parquet_files;

#[derive(Debug)]
pub enum Formats {
    Avro,
    Parquet,
}

pub fn dumpin(path: &str, thread: u32, format: Formats) -> Result<()> {
    let mut file_list: Vec<PathBuf> = vec![];
    get_all_parquet_files(path, &mut file_list);
    dumpin::start(&file_list, thread, format)?;
    Ok(())
}

pub fn dumpout(path: &str, thread: u32, format: Formats) -> Result<()> {
    dumpout::start(path, thread, format)?;
    Ok(())
}
