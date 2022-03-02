use parquet::basic::{ConvertedType, Type as PhysicalType};
use parquet::file::reader::{FileReader, SerializedFileReader};
use utils::{error::Result, taos::taos_connect};

#[tokio::main]
#[allow(unreachable_patterns)]
pub async fn start() -> Result<()> {
    let taos = taos_connect().unwrap();

    assert_eq!(
        taos.query("drop database if exists demo").await.is_ok(),
        true
    );

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
    assert_eq!(taos.query(sql.as_str()).await.is_ok(), true);

    Ok(())
}
