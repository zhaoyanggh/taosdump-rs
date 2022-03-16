use std::{
    fs::File,
    io::{BufReader, Write},
    path::PathBuf,
};

use avro_rs::{
    types::{Record, Value},
    Codec, Reader, Schema, Writer,
};
use bstr::BString;
use libtaos::Taos;
use serde_json::{self, json, Map};

pub fn generate_avro_schema(column_names: &Vec<String>, data_types: &Vec<BString>) -> Schema {
    let mut raw_json_schema = Map::new();
    raw_json_schema.insert(
        "type".to_string(),
        serde_json::Value::String("record".to_string()),
    );
    raw_json_schema.insert(
        "name".to_string(),
        serde_json::Value::String("m1".to_string()),
    );
    let mut field_json_array: Vec<serde_json::Value> = vec![];
    for i in 0..column_names.len() {
        let column = json!({ "name": column_names[i], "type": match data_types[i].to_string().as_str() {
            "TINYINT" | "TINYINT UNSIGNED" | "SMALLINT" | "SMALLINT UNSIGNED" | "INT" => "int",
            "INT UNSIGNED" | "BIGINT" | "TIMESTAMP" | "BIGINT UNSIGNED" => "long",
            "BOOL" => "boolean",
            "FLOAT" => "float",
            "DOUBLE" => "double",
            "BINARY" => "bytes",
            "NCHAR" => "string",
            _ => unreachable!("unexpected data type, please contact the author to fix!"),
        }});
        field_json_array.push(column);
    }
    raw_json_schema.insert(
        "fields".to_string(),
        serde_json::Value::Array(field_json_array),
    );

    println!("{:?}", serde_json::to_string(&raw_json_schema).unwrap());

    Schema::parse_str(serde_json::to_string(&raw_json_schema).unwrap().as_str()).unwrap()
}

pub fn avro_dumpout(
    mut file: File,
    column_names: &Vec<String>,
    data_types: &Vec<BString>,
    rows: Vec<Vec<libtaos::Field>>,
) -> i64 {
    let schema = generate_avro_schema(column_names, data_types);
    let mut writer = Writer::with_codec(&schema, Vec::new(), Codec::Deflate);
    let mut data_point = 0;
    for row in rows {
        let mut record = Record::new(writer.schema()).unwrap();
        let mut index = 0;
        for field in row {
            match field {
                libtaos::Field::Null => todo!(),
                libtaos::Field::Bool(v) => record.put(column_names[index].as_str(), v),
                libtaos::Field::TinyInt(v) => record.put(column_names[index].as_str(), v as i32),
                libtaos::Field::SmallInt(v) => record.put(column_names[index].as_str(), v as i32),
                libtaos::Field::Int(v) => record.put(column_names[index].as_str(), v),
                libtaos::Field::BigInt(v) => record.put(column_names[index].as_str(), v),
                libtaos::Field::Float(v) => record.put(column_names[index].as_str(), v),
                libtaos::Field::Double(v) => record.put(column_names[index].as_str(), v),
                libtaos::Field::Binary(v) => {
                    record.put(column_names[index].as_str(), (&*v).to_vec())
                }
                libtaos::Field::Timestamp(v) => {
                    record.put(column_names[index].as_str(), v.as_raw_timestamp())
                }
                libtaos::Field::NChar(v) => record.put(column_names[index].as_str(), v),
                libtaos::Field::UTinyInt(v) => record.put(column_names[index].as_str(), v as i32),
                libtaos::Field::USmallInt(v) => record.put(column_names[index].as_str(), v as i32),
                libtaos::Field::UInt(v) => record.put(column_names[index].as_str(), v as i64),
                libtaos::Field::UBigInt(v) => record.put(column_names[index].as_str(), v as i64),
            };
            index += 1;
            data_point += 1;
        }
        writer.append(record).unwrap();
    }
    let input = writer.into_inner().unwrap();
    file.write(&input).unwrap();
    data_point
}

#[tokio::main]
pub async fn avro_dumpin(file_list: &Vec<PathBuf>, taos: Taos) {
    assert_eq!(
        taos.query("create database if not exists demo")
            .await
            .is_ok(),
        true
    );
    assert_eq!(taos.query("use demo").await.is_ok(), true);
    assert_eq!(taos.query("create table m1 (ts timestamp,c1 tinyint,c2 tinyint unsigned,c3 smallint,c4 smallint unsigned,c5 int,c6 int unsigned,c7 bigint,c8 bigint unsigned,c9 float,c10 double,c11 binary(8),c12 nchar(8),c13 bool)").await.is_ok(), true);

    for file in file_list {
        let f = File::open(file).unwrap();
        let buffered_reader = BufReader::new(f);
        let r = Reader::new(buffered_reader);
        let mut sql = "insert into m1 values".to_string();
        for x in r.unwrap() {
            let mut _count = 0;
            sql += "(";
            match x.unwrap() {
                Value::Record(r) => {
                    for row in r.into_iter() {
                        if _count != 0 {
                            sql += ",";
                        }
                        _count += 1;
                        match row.1 {
                            Value::Null => todo!(),
                            Value::Boolean(v) => sql += &v.to_string(),
                            Value::Int(v) => sql += &v.to_string(),
                            Value::Long(v) => sql += &v.to_string(),
                            Value::Float(v) => sql += &v.to_string(),
                            Value::Double(v) => sql += &v.to_string(),
                            Value::Bytes(v) => sql += String::from_utf8(v).unwrap().as_str(),
                            Value::String(v) => sql += &v.to_string(),
                            _ => unreachable!(),
                        }
                    }
                }
                _ => unreachable!(),
            }
            sql += ")";
        }
        assert_eq!(taos.query(sql.as_str()).await.is_ok(), true);
    }
}
