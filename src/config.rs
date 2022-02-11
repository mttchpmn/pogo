use std::fs;
use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize)]
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

        let contents = match fs::read_to_string(&path) {
            Ok(contents) => contents,
            Err(_) => {
                println!("No config file found. Creating one for you at '~/.pogo/pogo.toml'");
                Config::create_default_config();
                fs::read_to_string(&path).expect("Couldn't read config file at `~/.pogo/pogo.toml`")
            }
        };

        let config: PogoConfig = toml::from_str(&contents).unwrap();

        config
    }

    fn create_default_config() {
        let home = std::env::var("HOME").unwrap();
        let path = home + "/.pogo/pogo.toml";

        let default_config = PogoConfig {
            connection_string: "".to_string()
        };

        let toml = toml::to_string(&default_config).unwrap();

        fs::write(path, toml);
    }
}
