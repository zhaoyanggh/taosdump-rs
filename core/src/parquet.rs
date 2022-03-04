use bstr::BString;
use libtaos::*;
use parquet::basic::{
    ConvertedType, LogicalType, Repetition, TimeUnit, TimestampType, Type as PhysicalType,
};
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::schema::types::*;
use parquet::{
    column::writer::ColumnWriter,
    file::{
        properties::WriterProperties,
        writer::{FileWriter, SerializedFileWriter},
    },
};
use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;
fn generate_parquet_schema(column_names: &Vec<String>, data_types: &Vec<BString>) -> Arc<Type> {
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

    Arc::new(
        Type::group_type_builder("schema")
            .with_fields(&mut fields)
            .build()
            .unwrap(),
    )
}

pub fn parquet_dumpout(
    file: File,
    column_names: &Vec<String>,
    data_types: &Vec<BString>,
    rows: Vec<Vec<libtaos::Field>>,
) -> i64 {
    let schema = generate_parquet_schema(&column_names, &data_types);
    let props = Arc::new(WriterProperties::builder().build());
    let mut writer = SerializedFileWriter::new(file, schema, props).unwrap();

    let mut num_points: i64 = 0;

    for row in rows {
        let mut row_group_writer = writer.next_row_group().unwrap();
        for field in row {
            let data_writer = row_group_writer.next_column().unwrap();
            if let Some(mut writer) = data_writer {
                match writer {
                    ColumnWriter::Int32ColumnWriter(ref mut typed) => {
                        let values;
                        match field {
                            Field::Null => todo!(),
                            Field::TinyInt(v) => {
                                values = vec![v as i32];
                            }
                            Field::UTinyInt(v) => {
                                values = vec![v as i32];
                            }
                            Field::SmallInt(v) => {
                                values = vec![v as i32];
                            }
                            Field::USmallInt(v) => {
                                values = vec![v as i32];
                            }
                            Field::Int(v) => {
                                values = vec![v];
                            }
                            Field::UInt(v) => {
                                values = vec![v as i32];
                            }
                            _ => unreachable!(
                                "unexpected data type, please contact the author to fix!"
                            ),
                        }

                        num_points += typed.write_batch(&values[..], None, None).unwrap() as i64;
                    }
                    ColumnWriter::BoolColumnWriter(ref mut typed) => {
                        let values = vec![*field.as_bool().unwrap()];
                        num_points += typed.write_batch(&values[..], None, None).unwrap() as i64;
                    }
                    ColumnWriter::Int64ColumnWriter(ref mut typed) => {
                        let values;
                        match field {
                            Field::Null => todo!(),
                            Field::BigInt(v) => {
                                values = vec![v];
                            }
                            Field::Timestamp(v) => {
                                values = vec![v.as_raw_timestamp()];
                            }
                            Field::UBigInt(v) => {
                                values = vec![v as i64];
                            }
                            _ => unreachable!(
                                "unexpected data type, please contact the author to fix!"
                            ),
                        }
                        num_points += typed.write_batch(&values[..], None, None).unwrap() as i64;
                    }
                    ColumnWriter::FloatColumnWriter(ref mut typed) => {
                        let values = vec![*field.as_float().unwrap()];
                        num_points += typed.write_batch(&values[..], None, None).unwrap() as i64;
                    }
                    ColumnWriter::DoubleColumnWriter(ref mut typed) => {
                        let values = vec![*field.as_double().unwrap()];
                        num_points += typed.write_batch(&values[..], None, None).unwrap() as i64;
                    }
                    ColumnWriter::ByteArrayColumnWriter(ref mut typed) => {
                        let values;
                        match field {
                            Field::Null => todo!(),
                            Field::Binary(v) => {
                                let binary_data = (&*v).to_vec();
                                values = vec![parquet::data_type::ByteArray::from(binary_data)];
                            }
                            Field::NChar(_) => {
                                values = vec![parquet::data_type::ByteArray::from(
                                    field.as_nchar().unwrap(),
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
    num_points
}
#[tokio::main]
pub async fn parquet_dumpin(file_list: &Vec<PathBuf>, taos: Taos) {
    for file in file_list {
        let parquet_reader = SerializedFileReader::try_from(file.to_str().unwrap()).unwrap();
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
                        _ => {
                            unreachable!("unexpected data type, please contact the author to fix!")
                        }
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
        assert_eq!(taos.query(sql.as_str()).await.is_ok(), true);
    }
}
