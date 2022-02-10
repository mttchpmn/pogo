use std::fs;
use serde::Deserialize;

#[derive(Deserialize)]
struct PogoConfig {
   connection_string: String,
}

pub struct Config {}

impl Config {
    pub fn ensure_pogo_dir_exists() {
        let home = std::env::var("HOME").unwrap();
        let path = format!("{}/.pogo/operations", home);
        fs::create_dir_all(path).expect("Error creating Pogo directory");
    }

    pub fn get_connection_string() -> String {
        let config = Config::load_pogo_config();

        config.connection_string
    }

    fn load_pogo_config() -> PogoConfig {
        let home = std::env::var("HOME").unwrap();
        let path = home + "/.pogo/pogo.toml";
        let contents = fs::read_to_string(path).expect("Couldnt read config file");
        let config: PogoConfig = toml::from_str(&contents).unwrap();

        config
    }
}