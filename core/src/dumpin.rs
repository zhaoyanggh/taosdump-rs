use std::path::PathBuf;

use utils::{error::Result, taos::taos_connect};

use crate::{commands::Formats, parquet::parquet_dumpin};

#[tokio::main]
pub async fn start(file_list: &Vec<PathBuf>, thread: u32, format: Formats) -> Result<()> {
    println!("threads: {}", thread);
    let taos = taos_connect().unwrap();

    assert_eq!(
        taos.query("create database if not exists demo")
            .await
            .is_ok(),
        true
    );
    assert_eq!(taos.query("use demo").await.is_ok(), true);

    match format {
        Formats::Parquet => parquet_dumpin(file_list, taos),
        Formats::Avro => todo!(),
    };

    Ok(())
}
