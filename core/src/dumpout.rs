use libtaos::*;
use utils::Formats;

use std::{fs, path::Path};
use utils::error::Result;
use utils::taos::taos_connect;

use crate::avro::avro_dumpout;
use crate::parquet::parquet_dumpout;

#[tokio::main]
pub async fn start(dir_path: &str, _threads: u32, format: Formats) -> Result<i64> {
    let taos = taos_connect().unwrap();

    let mut data_types = vec![];
    let rows = taos.query("describe demo.m1").await.unwrap();
    for row in rows.rows {
        match row[1].clone() {
            Field::Binary(v) => {
                data_types.push(v);
            }
            _ => {
                unreachable!("unexpected data type, please contact the author to fix!");
            }
        }
    }

    let rows = taos.query("select * from demo.m1").await.unwrap();

    let mut column_names = vec![];
    for row in rows.column_meta {
        column_names.push(row.name);
    }

    assert_eq!(column_names.len(), data_types.len());

    let path = Path::new(dir_path);

    let file = fs::File::create(&path).unwrap();

    let num_of_points = match format {
        Formats::Parquet => parquet_dumpout(file, &column_names, &data_types, rows.rows),
        Formats::Avro => avro_dumpout(file, &column_names, &data_types, rows.rows),
    };

    Ok(num_of_points)
}
