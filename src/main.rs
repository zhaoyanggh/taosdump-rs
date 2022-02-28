use libtaos::*;
use parquet::file::reader::SerializedFileReader;
use parquet::{
    column::writer::ColumnWriter,
    data_type::ByteArray,
    file::{
        properties::WriterProperties,
        writer::{FileWriter, SerializedFileWriter},
    },
    schema::parser::parse_message_type,
};
use std::convert::TryFrom;
use std::env::var;
use std::{fs, path::Path, sync::Arc};

#[tokio::main]
async fn main() -> Result<(), Error> {
    let taos = taos_connect()?;

    assert_eq!(
        taos.query("drop database if exists demo").await.is_ok(),
        true
    );

    let path = Path::new("./sample.parquet");

    let message_type = "
        message schema {
            REQUIRED INT32 b;
            REQUIRED BINARY msg (UTF8);
        }
    ";
    let schema = Arc::new(parse_message_type(message_type).unwrap());
    let props = Arc::new(WriterProperties::builder().build());
    let file = fs::File::create(&path).unwrap();

    let mut rows: i64 = 0;
    let data = vec![(10, "A"), (20, "C"), (30, "C"), (40, "D")];

    let mut writer = SerializedFileWriter::new(file, schema, props).unwrap();
    for (key, value) in data {
        println!("key: {}, value: {}\n", key, value);
        let mut row_group_writer = writer.next_row_group().unwrap();
        let id_writer = row_group_writer.next_column().unwrap();
        if let Some(mut writer) = id_writer {
            match writer {
                ColumnWriter::Int32ColumnWriter(ref mut typed) => {
                    let values = vec![key];
                    rows += typed.write_batch(&values[..], None, None).unwrap() as i64;
                }
                _ => {
                    unimplemented!();
                }
            }
            row_group_writer.close_column(writer).unwrap();
        }
        let data_writer = row_group_writer.next_column().unwrap();
        if let Some(mut writer) = data_writer {
            match writer {
                ColumnWriter::ByteArrayColumnWriter(ref mut typed) => {
                    let values = ByteArray::from(value);
                    rows += typed.write_batch(&[values], None, None).unwrap() as i64;
                }
                _ => {
                    unimplemented!();
                }
            }
            row_group_writer.close_column(writer).unwrap();
        }
        writer.close_row_group(row_group_writer).unwrap();
    }
    writer.close().unwrap();

    println!("Wrote {}", rows);

    // let paths = vec!["./sample.parquet"];
    // let rows = paths
    //     .iter()
    //     .map(|p| SerializedFileReader::try_from(*p).unwrap())
    //     .flat_map(|r| r.into_iter());

    let paths = "./sample.parquet";
    let rows = SerializedFileReader::try_from(paths).unwrap();
    for row in rows {
        println!("{}", row);
    }
    Ok(())
}

fn var_or_default(env: &str, default: &str) -> String {
    var(env).unwrap_or(default.to_string())
}

fn taos_connect() -> Result<Taos, Error> {
    TaosCfgBuilder::default()
        .ip(&var_or_default("TEST_TAOS_IP", "127.0.0.1"))
        .user(&var_or_default("TEST_TAOS_USER", "root"))
        .pass(&var_or_default("TEST_TAOS_PASS", "taosdata"))
        .db(&var_or_default("TEST_TAOS_DB", "log"))
        .port(
            var_or_default("TEST_TAOS_PORT", "6030")
                .parse::<u16>()
                .unwrap(),
        )
        .build()
        .expect("TaosCfg builder error")
        .connect()
}
