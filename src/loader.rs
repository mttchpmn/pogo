pub struct Operation {
    pub name: String,
    pub description: String,
    pub command: String,
}

pub struct Loader {}

impl Loader {
    pub fn get_operations() -> Vec<Operation> {
        let example_operation = Operation {
            name: "get-client-names".to_string(),
            description: "Gets all client names".to_string(),
            command: "".to_string()
        };

        vec![example_operation]
    }

    pub fn get_operation(operation_name: &str) -> Operation {
        let example_operation = Operation {
            name: "get-client-names".to_string(),
            description: "Gets all client names".to_string(),
            command: "SELECT long_name FROM client".to_string()
        };

        example_operation
    }
}