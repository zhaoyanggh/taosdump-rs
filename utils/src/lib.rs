pub mod error;
pub mod taos;

use std::path::PathBuf;
#[derive(Debug)]
pub enum Formats {
    Avro,
    Parquet,
}
use glob::glob;

pub fn get_all_data_files(dir_path: &str, file_list: &mut Vec<PathBuf>, format: &Formats) {
    let paths;
    if dir_path.ends_with("/") {
        match *format {
            Formats::Avro => {
                paths = glob(format!("{}*.avro", dir_path).as_str())
                    .expect("Failed to read glob pattern")
            }
            Formats::Parquet => {
                paths = glob(format!("{}*.parquet", dir_path).as_str())
                    .expect("Failed to read glob pattern")
            }
        }
    } else {
        match *format {
            Formats::Avro => {
                paths = glob(format!("{}/*.avro", dir_path).as_str())
                    .expect("Failed to read glob pattern")
            }
            Formats::Parquet => {
                paths = glob(format!("{}/*.parquet", dir_path).as_str())
                    .expect("Failed to read glob pattern")
            }
        }
    }
    for entry in paths {
        match entry {
            Ok(path) => {
                println!("{:?}", path);
                file_list.push(path)
            }
            Err(e) => println!("{:?}", e),
        }
    }
}
