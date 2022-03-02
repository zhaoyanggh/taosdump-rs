use libtaos::*;
use parquet::basic::{
    ConvertedType, LogicalType, Repetition, TimeUnit, TimestampType, Type as PhysicalType,
};

use parquet::schema::types::*;
use parquet::{
    column::writer::ColumnWriter,
    file::{
        properties::WriterProperties,
        writer::{FileWriter, SerializedFileWriter},
    },
};
use std::{fs, path::Path, sync::Arc};
use utils::error::Result;
use utils::taos::taos_connect;

#[tokio::main]
#[allow(unreachable_patterns)]
pub async fn start() -> Result<()> {
    let taos = taos_connect().unwrap();

    assert_eq!(taos.query("create database demo").await.is_ok(), true);
    assert_eq!(taos.query("use demo").await.is_ok(), true);

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
    // println!("{:?}", data_types);

    let rows = taos.query("select * from m1").await.unwrap();

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

    Ok(())
}
