pub mod error;
pub mod taos;

use std::path::PathBuf;

use glob::glob;

pub fn get_all_parquet_files(dir_path: &str, file_list: &mut Vec<PathBuf>) {
    let paths;
    if dir_path.ends_with("/") {
        paths =
            glob(format!("{}*.parquet", dir_path).as_str()).expect("Failed to read glob pattern");
    } else {
        paths =
            glob(format!("{}/*.parquet", dir_path).as_str()).expect("Failed to read glob pattern");
    }
    for entry in paths {
        match entry {
            Ok(path) => file_list.push(path),
            Err(e) => println!("{:?}", e),
        }
    }
}
