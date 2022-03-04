use std::{
    fs::File,
    io::{BufReader, Write},
    ops::Residual,
    path::PathBuf,
};

use avro_rs::types::Value;
use avro_rs::{types::Record, Codec, Reader, Schema, Writer};
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

    let file = File::open("sample.avro").unwrap();
    get_schema(file);
    let file = File::open("sample.avro").unwrap();
    data_point
}

fn get_schema(file: File) {
    let buffered_reader = BufReader::new(file);

    let r = Reader::new(buffered_reader);
    for x in r {
        let json: serde_json::Value =
            serde_json::from_str(&x.writer_schema().canonical_form()).expect("");
        let pretty = serde_json::to_string_pretty(&json).expect("");
        println!("{}", pretty);
    }
}

fn avro_to_json(x: Value) -> serde_json::Value {
    match x {
        Value::Null => serde_json::Value::Null,
        Value::Boolean(b) => serde_json::Value::Bool(b),
        Value::Long(n) => serde_json::json!(n),
        Value::Double(n) => serde_json::json!(n),
        Value::Int(n) => serde_json::json!(n),
        Value::Float(n) => serde_json::json!(n),
        Value::Fixed(n, items) => serde_json::json!(vec![
            serde_json::json!(n),
            serde_json::Value::Array(
                items
                    .into_iter()
                    .map(|item| serde_json::json!(item))
                    .collect()
            )
        ]),

        Value::Bytes(items) => serde_json::Value::Array(
            items
                .into_iter()
                .map(|item| serde_json::json!(item))
                .collect(),
        ),
        Value::String(s) => serde_json::Value::String(s),
        Value::Array(items) => {
            serde_json::Value::Array(items.into_iter().map(|item| avro_to_json(item)).collect())
        }
        Value::Map(items) => serde_json::Value::Object(
            items
                .into_iter()
                .map(|(key, value)| (key, avro_to_json(value)))
                .collect::<_>(),
        ),
        Value::Record(items) => serde_json::Value::Object(
            items
                .into_iter()
                .map(|(key, value)| (key, avro_to_json(value)))
                .collect::<_>(),
        ),
        Value::Union(v) => avro_to_json(*v),
        Value::Enum(_v, s) => serde_json::json!(s),
        Value::Date(_) => todo!(),
        Value::Decimal(_) => todo!(),
        Value::TimeMillis(_) => todo!(),
        Value::TimeMicros(_) => todo!(),
        Value::TimestampMillis(_) => todo!(),
        Value::TimestampMicros(_) => todo!(),
        Value::Duration(_) => todo!(),
        Value::Uuid(_) => todo!(),
    }
}

#[tokio::main]
pub async fn avro_dumpin(file_list: &Vec<PathBuf>, taos: Taos) {
    taos.query("create database if not exists demo").await;
    taos.query("use demo").await;
    taos.query("create table m1 (ts timestamp,c1 tinyint,c2 tinyint unsigned,c3 smallint,c4 smallint unsigned,c5 int,c6 int unsigned,c7 bigint,c8 bigint unsigned,c9 float,c10 double,c11 binary(8),c12 nchar(8),c13 bool)").await;
    for file in file_list {
        let f = File::open(file).unwrap();
        let buffered_reader = BufReader::new(f);
        let r = Reader::new(buffered_reader);
        for x in r.unwrap() {
            let json = avro_to_json(x.unwrap());
            println!("{}\n", json);
        }
    }
}
