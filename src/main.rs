use itertools::Itertools;
use libtaos::*;
use parquet::arrow::{ArrowReader, ParquetFileArrowReader};
use parquet::basic::{
    ConvertedType, LogicalType, Repetition, TimeUnit, TimestampType, Type as PhysicalType,
};
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::schema::types::*;
use parquet::{column, data_type};
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
#[allow(unreachable_patterns)]
async fn main() -> Result<(), Error> {
    let taos = taos_connect()?;

    assert_eq!(
        taos.query("drop database if exists demo").await.is_ok(),
        true
    );

    assert_eq!(taos.query("create database demo").await.is_ok(), true);
    assert_eq!(taos.query("use demo").await.is_ok(), true);
    assert_eq!(
        taos.query("create table m1 (ts timestamp, c1 tinyint, c2 tinyint unsigned, c3 smallint, c4 smallint unsigned, c5 int, c6 int unsigned, c7 bigint, c8 bigint unsigned, c9 float, c10 double, c11 binary(8), c12 nchar(8), c13 bool)")
            .await
            .is_ok(),
        true
    );

    println!("create table m1 (ts timestamp, c1 tinyint, c2 tinyint unsigned, c3 smallint, c4 smallint unsigned, c5 int, c6 int unsigned, c7 bigint, c8 bigint unsigned, c9 float, c10 double, c11 binary(8), c12 nchar(8), c13 bool)");

    for i in 0..10i32 {
        assert_eq!(
            taos.query(
                format!(
                    "insert into m1 values (now+{}s, {}, {}, {}, {},{},{},{},{},{},{},'{}','{}',1)",
                    i, i, i, i, i, i, i, i, i, i, i, i, i
                )
                .as_str()
            )
            .await
            .is_ok(),
            true
        );
    }

    // let describe = taos.describe("m1").await.unwrap();

    // for column in describe.cols {
    //     println!("{:?}", column);
    // }

    let mut data_types = vec![];
    let rows = taos.query("describe m1").await.unwrap();
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
    // println!("{:?}", data_types);

    let rows = taos.query("select * from m1").await?;

    assert_eq!(
        taos.query("drop database if exists demo").await.is_ok(),
        true
    );

    let mut column_names = vec![];
    for row in rows.column_meta {
        column_names.push(row.name);
    }
    // println!("{:?}", column_names);

    assert_eq!(column_names.len(), data_types.len());

    let path = Path::new("./sample.parquet");

    let props = Arc::new(WriterProperties::builder().build());
    let file = fs::File::create(&path).unwrap();

    let mut num_points: i64 = 0;

    let mut fields = vec![];

    for i in 0..column_names.len() {
        match data_types[i].to_string().as_str() {
            "TINYINT" => fields.push(Arc::new(
                Type::primitive_type_builder(&column_names[i], PhysicalType::INT32)
                    .with_repetition(Repetition::REQUIRED)
                    .with_converted_type(ConvertedType::INT_8)
                    .build()
                    .unwrap(),
            )),
            "TINYINT UNSIGNED" => fields.push(Arc::new(
                Type::primitive_type_builder(&column_names[i], PhysicalType::INT32)
                    .with_repetition(Repetition::REQUIRED)
                    .with_converted_type(ConvertedType::UINT_8)
                    .build()
                    .unwrap(),
            )),
            "SMALLINT" => fields.push(Arc::new(
                Type::primitive_type_builder(&column_names[i], PhysicalType::INT32)
                    .with_repetition(Repetition::REQUIRED)
                    .with_converted_type(ConvertedType::INT_16)
                    .build()
                    .unwrap(),
            )),
            "SMALLINT UNSIGNED" => fields.push(Arc::new(
                Type::primitive_type_builder(&column_names[i], PhysicalType::INT32)
                    .with_repetition(Repetition::REQUIRED)
                    .with_converted_type(ConvertedType::UINT_16)
                    .build()
                    .unwrap(),
            )),
            "INT" => fields.push(Arc::new(
                Type::primitive_type_builder(&column_names[i], PhysicalType::INT32)
                    .with_repetition(Repetition::REQUIRED)
                    .build()
                    .unwrap(),
            )),
            "INT UNSIGNED" => fields.push(Arc::new(
                Type::primitive_type_builder(&column_names[i], PhysicalType::INT32)
                    .with_repetition(Repetition::REQUIRED)
                    .with_converted_type(ConvertedType::UINT_32)
                    .build()
                    .unwrap(),
            )),
            "BIGINT" => fields.push(Arc::new(
                Type::primitive_type_builder(&column_names[i], PhysicalType::INT64)
                    .with_repetition(Repetition::REQUIRED)
                    .build()
                    .unwrap(),
            )),
            "TIMESTAMP" => fields.push(Arc::new(
                Type::primitive_type_builder(&column_names[i], PhysicalType::INT64)
                    .with_repetition(Repetition::REQUIRED)
                    .with_logical_type(Some(LogicalType::TIMESTAMP(TimestampType {
                        is_adjusted_to_u_t_c: false,
                        unit: TimeUnit::MILLIS(Default::default()),
                    })))
                    .build()
                    .unwrap(),
            )),
            "BIGINT UNSIGNED" => fields.push(Arc::new(
                Type::primitive_type_builder(&column_names[i], PhysicalType::INT64)
                    .with_repetition(Repetition::REQUIRED)
                    .with_converted_type(ConvertedType::UINT_64)
                    .build()
                    .unwrap(),
            )),
            "FLOAT" => fields.push(Arc::new(
                Type::primitive_type_builder(&column_names[i], PhysicalType::FLOAT)
                    .with_repetition(Repetition::REQUIRED)
                    .build()
                    .unwrap(),
            )),
            "DOUBLE" => fields.push(Arc::new(
                Type::primitive_type_builder(&column_names[i], PhysicalType::DOUBLE)
                    .with_repetition(Repetition::REQUIRED)
                    .build()
                    .unwrap(),
            )),
            "BINARY" => fields.push(Arc::new(
                Type::primitive_type_builder(&column_names[i], PhysicalType::BYTE_ARRAY)
                    .with_repetition(Repetition::REQUIRED)
                    .with_id(8)
                    .build()
                    .unwrap(),
            )),
            "NCHAR" => fields.push(Arc::new(
                Type::primitive_type_builder(&column_names[i], PhysicalType::BYTE_ARRAY)
                    .with_repetition(Repetition::REQUIRED)
                    .with_logical_type(Some(LogicalType::STRING(Default::default())))
                    .with_id(8)
                    .build()
                    .unwrap(),
            )),
            "BOOL" => fields.push(Arc::new(
                Type::primitive_type_builder(&column_names[i], PhysicalType::BOOLEAN)
                    .with_repetition(Repetition::REQUIRED)
                    .build()
                    .unwrap(),
            )),
            _ => unreachable!("unexpected data type, please contact the author to fix!"),
        }
    }

    let schema = Arc::new(
        Type::group_type_builder("schema")
            .with_fields(&mut fields)
            .build()
            .unwrap(),
    );

    let mut writer = SerializedFileWriter::new(file, schema, props).unwrap();

    for row in rows.rows {
        let mut row_group_writer = writer.next_row_group().unwrap();
        for i in 0..row.len() {
            let data_writer = row_group_writer.next_column().unwrap();
            if let Some(mut writer) = data_writer {
                match writer {
                    ColumnWriter::Int32ColumnWriter(ref mut typed) => {
                        let values;
                        match row[i] {
                            Field::Null => todo!(),
                            Field::TinyInt(_) => {
                                values = vec![*row[i].as_tiny_int().unwrap() as i32];
                            }
                            Field::UTinyInt(_) => {
                                values = vec![*row[i].as_unsigned_tiny_int().unwrap() as i32];
                            }
                            Field::SmallInt(_) => {
                                values = vec![*row[i].as_small_int().unwrap() as i32];
                            }
                            Field::USmallInt(_) => {
                                values = vec![*row[i].as_unsigned_samll_int().unwrap() as i32];
                            }
                            Field::Int(_) => {
                                values = vec![*row[i].as_int().unwrap()];
                            }
                            Field::UInt(_) => {
                                values = vec![*row[i].as_unsigned_int().unwrap() as i32];
                            }
                            _ => unreachable!(
                                "unexpected data type, please contact the author to fix!"
                            ),
                        }

                        num_points += typed.write_batch(&values[..], None, None).unwrap() as i64;
                    }
                    ColumnWriter::BoolColumnWriter(ref mut typed) => {
                        let values = vec![*row[i].as_bool().unwrap()];
                        num_points += typed.write_batch(&values[..], None, None).unwrap() as i64;
                    }
                    ColumnWriter::Int64ColumnWriter(ref mut typed) => {
                        let values;
                        match row[i] {
                            Field::Null => todo!(),
                            Field::BigInt(_) => {
                                values = vec![*row[i].as_big_int().unwrap()];
                            }
                            Field::Timestamp(_) => {
                                values = vec![row[i].as_timestamp().unwrap().as_raw_timestamp()];
                            }
                            Field::UBigInt(_) => {
                                values = vec![*row[i].as_unsigned_big_int().unwrap() as i64];
                            }
                            _ => unreachable!(
                                "unexpected data type, please contact the author to fix!"
                            ),
                        }
                        num_points += typed.write_batch(&values[..], None, None).unwrap() as i64;
                    }
                    ColumnWriter::FloatColumnWriter(ref mut typed) => {
                        let values = vec![*row[i].as_float().unwrap()];
                        num_points += typed.write_batch(&values[..], None, None).unwrap() as i64;
                    }
                    ColumnWriter::DoubleColumnWriter(ref mut typed) => {
                        let values = vec![*row[i].as_double().unwrap()];
                        num_points += typed.write_batch(&values[..], None, None).unwrap() as i64;
                    }
                    ColumnWriter::ByteArrayColumnWriter(ref mut typed) => {
                        let values;
                        match row[i] {
                            Field::Null => todo!(),
                            Field::Binary(_) => {
                                let binary_data = (&*row[i].as_binary().unwrap()).to_vec();
                                values = vec![parquet::data_type::ByteArray::from(binary_data)];
                            }
                            Field::NChar(_) => {
                                values = vec![parquet::data_type::ByteArray::from(
                                    row[i].as_nchar().unwrap(),
                                )];
                            }
                            _ => unreachable!(
                                "unexpected data type, please contact the author to fix!"
                            ),
                        }

                        num_points += typed.write_batch(&values[..], None, None).unwrap() as i64;
                    }
                    ColumnWriter::FixedLenByteArrayColumnWriter(_) => todo!(),
                    _ => unreachable!("unexpected data type, please contact the author to fix!"),
                }
                row_group_writer.close_column(writer).unwrap();
            }
        }
        writer.close_row_group(row_group_writer).unwrap();
    }
    writer.close().unwrap();

    println!("Wrote {}", num_points);

    // let paths = vec!["./sample.parquet"];
    // let rows = paths
    //     .iter()
    //     .map(|p| SerializedFileReader::try_from(*p).unwrap())
    //     .flat_map(|r| r.into_iter());

    let paths = "./sample.parquet";
    let parquet_reader = SerializedFileReader::try_from(paths).unwrap();
    assert_eq!(taos.query("create database demo").await.is_ok(), true);
    assert_eq!(taos.query("use demo").await.is_ok(), true);
    let mut sql = "create table m1 (".to_string();
    let read_schema = parquet_reader.metadata().file_metadata().schema();
    let mut index = 0;
    for field in read_schema.get_fields() {
        if index != 0 {
            sql += ",";
        }
        index += 1;
        sql += field.get_basic_info().name();
        match field.get_physical_type() {
            PhysicalType::BOOLEAN => sql += " bool",
            PhysicalType::INT32 => match field.get_basic_info().converted_type() {
                ConvertedType::UINT_8 => sql += " tinyint unsigned",
                ConvertedType::UINT_16 => sql += " smallint unsigned",
                ConvertedType::UINT_32 => sql += " int unsigned",
                ConvertedType::INT_8 => sql += " tinyint",
                ConvertedType::INT_16 => sql += " smallint",
                ConvertedType::NONE => sql += " int",
                _ => unreachable!("unexpected data type, please contact the author to fix!"),
            },
            PhysicalType::INT64 => match field.get_basic_info().logical_type() {
                Some(_) => sql += " timestamp",
                None => match field.get_basic_info().converted_type() {
                    ConvertedType::NONE => sql += " bigint",
                    ConvertedType::UINT_64 => sql += " bigint unsigned",
                    _ => unreachable!("unexpected data type, please contact the author to fix!"),
                },
            },
            PhysicalType::FLOAT => sql += " float",
            PhysicalType::DOUBLE => sql += " double",
            PhysicalType::BYTE_ARRAY => {
                let length = field.get_basic_info().id();
                match field.get_basic_info().logical_type() {
                    Some(_) => sql += &format!(" nchar({})", length),
                    None => sql += &format!(" binary({})", length),
                }
            }
            _ => unreachable!("unexpected data type, please contact the author to fix!"),
        }
    }
    sql += ")";
    println!("sql: {}", sql);
    assert_eq!(taos.query(sql.as_str()).await.is_ok(), true);
    let mut sql = "insert into m1 values".to_string();
    for row in parquet_reader {
        sql += "(";
        let mut count = 0;
        for (_, col) in row.get_column_iter() {
            if count != 0 {
                sql += ",";
            }
            count += 1;
            match col {
                parquet::record::Field::Null => todo!(),
                parquet::record::Field::Bool(v) => sql += &v.to_string(),
                parquet::record::Field::Byte(v) => sql += &v.to_string(),
                parquet::record::Field::Short(v) => sql += &v.to_string(),
                parquet::record::Field::Int(v) => sql += &v.to_string(),
                parquet::record::Field::Long(v) => sql += &v.to_string(),
                parquet::record::Field::UByte(v) => sql += &v.to_string(),
                parquet::record::Field::UShort(v) => sql += &v.to_string(),
                parquet::record::Field::UInt(v) => sql += &v.to_string(),
                parquet::record::Field::ULong(v) => sql += &v.to_string(),
                parquet::record::Field::Float(v) => sql += &v.to_string(),
                parquet::record::Field::Double(v) => sql += &v.to_string(),
                parquet::record::Field::Str(v) => sql += format!("\'{}\'", &v).as_str(),
                parquet::record::Field::Bytes(v) => {
                    sql += format!("\'{}\'", std::str::from_utf8(v.data()).unwrap()).as_str()
                }
                parquet::record::Field::TimestampMillis(v) => sql += &v.to_string(),
                parquet::record::Field::TimestampMicros(v) => sql += &v.to_string(),
                parquet::record::Field::Group(v) => sql += &v.to_string(),
                _ => todo!(),
            }
        }
        sql += ")"
    }
    println!("{}", sql);
    assert_eq!(taos.query(sql.as_str()).await.is_ok(), true);
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
