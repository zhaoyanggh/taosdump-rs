use libtaos::*;
use log::{info, trace};
use utils::Formats;

use std::io::Write;
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

#[tokio::main]
pub async fn dumpout_database_sql(dir_path: &str, name: String) -> Result<()> {
    let taos = taos_connect().unwrap();
    let rows = taos
        .query(format!("show create database {}", name).as_str())
        .await
        .unwrap();
    let file_name = format!("{}{}.db", dir_path, name);
    let path = Path::new(file_name.as_str());
    let mut file = fs::File::create(&path).unwrap();
    for row in rows.rows {
        for field in row {
            match field {
                Field::Binary(v) => {
                    if v == name {
                        continue;
                    } else {
                        file.write(&v).unwrap();
                    }
                }
                _ => unreachable!(),
            }
        }
    }
    info!(
        "database {} is successfully dumped out to {}",
        name, file_name
    );
    Ok(())
}

#[tokio::main]
pub async fn dumpout_stable_sql(_dir_path: &str, name: String) -> Result<()> {
    let taos = taos_connect().unwrap();
    trace!("taos successfully connected");
    taos.use_database(&name).await.unwrap();
    trace!("use database {}", name);
    let rows = taos.query("show stables").await.unwrap();
    if rows.rows.len() == 0 {
        info!("no stable found!");
        return Ok(());
    } else {
        trace!("found stables");
        for row in rows.rows {
            assert!(row.len() > 1);
            for field in row {
                println!("{}", field);
                match field {
                    Field::Binary(v) => {
                        println!("{}", v);
                    }
                    _ => continue,
                }
            }
        }
    }
    Ok(())
}
