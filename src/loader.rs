use std::{fs, io};
use std::path::{Path, PathBuf};
use serde::Deserialize;

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Operation {
    pub name: String,
    pub description: String,
    pub command: String,
}

pub struct Loader {}

impl Loader {
    pub fn get_operations() -> Vec<Operation> {
        let file_paths = Loader::get_file_paths();

        let mut operations = vec![];
        for file_path in file_paths {
            let operation = Loader::get_operation(file_path);
            operations.push(operation);
        }

        operations
    }

    pub fn get_operation(file_path: PathBuf) -> Operation {
        let content = fs::read_to_string(file_path).expect("Couldn't read operation");
        let operation: Operation = serde_json::from_str(&content).expect("Couldn't parse operation");

        operation
    }

    fn get_file_paths() -> Vec<PathBuf> {
        let home = std::env::var("HOME").unwrap();
        let path =  home + "/.pogo/operations";

        let file_paths = fs::read_dir(path)
            .expect("Couldn't read `operations` directory")
            .map(|x| { x.map(|y| {y.path()})})
            .collect::<Result<Vec<PathBuf>, io::Error>>().expect("Couldn't read files in directory");

        file_paths
    }
}