use std::path::PathBuf;

use utils::{error::Result, taos::taos_connect, Formats};

use crate::{avro::avro_dumpin, parquet::parquet_dumpin};

pub fn start(file_list: &Vec<PathBuf>, _thread: u32, format: Formats) -> Result<()> {
    let taos = taos_connect().unwrap();
    taos.raw_query("create database if not exists demo")?;
    taos.raw_query("use demo")?;
    match format {
        Formats::Parquet => parquet_dumpin(file_list, taos),
        Formats::Avro => avro_dumpin(file_list, taos),
    };

    Ok(())
}
