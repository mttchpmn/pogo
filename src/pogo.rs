use std::fmt::Display;
use std::path::PathBuf;

use postgres::{Client, NoTls, Row};
use postgres::types::{FromSql, Type};
use uuid::Uuid;

use crate::{Loader, Operation};

pub struct PogoResult {
    pub header: Vec<String>,
    pub rows: Vec<Vec<String>>,
}

pub struct Pogo {
    client: Client,
    operations: Vec<Operation>
}

impl Pogo {
    pub fn new(connection_string: &str) -> Self {
        let client = Client::connect(connection_string, NoTls).expect("Error connecting to DB");
        let operations = Loader::get_operations();

        Pogo { client, operations }
    }

    pub fn describe(&mut self, table_name: Option<&str>) -> PogoResult {
        let result = match table_name {
            Some(table_name) => self.describe_table(table_name),
            None => self.describe_database()
        };

        result
    }

    pub fn list(&mut self) -> PogoResult {
        let header = vec!["OPERATION NAME".to_string(), "DESCRIPTION".to_string()];
        let mut rows = vec![];

        for operation in &self.operations {
            let row = vec![operation.name.clone(), operation.description.clone()];
            rows.push(row);
        }

        PogoResult { header, rows}
    }

    pub fn run(&mut self, operation_name: &str) -> PogoResult {
        let operation = self.get_operation(operation_name);

        self.run_query(&operation.command)
    }

    fn get_operation(&self, operation_name: &str) -> Operation {
        // TODO - Remove need to clone in this method
        let Operation{name, command, description} = &self.operations.iter().find(|x| {x.name == operation_name}).expect("Couldn't find operation");

        return Operation { name: name.clone(), command: command.clone(), description: description.clone()}
    }

    fn describe_table(&mut self, table_name: &str) -> PogoResult {
        let sql = format!("\
SELECT
   table_name,
   column_name,
   data_type
FROM
   information_schema.columns
WHERE
   table_name = '{}';", table_name);

        self.run_query(&sql)
    }

    fn describe_database(&mut self) -> PogoResult {
        let describe_sql = "\
SELECT n.nspname as \"Schema\",
  c.relname as \"Name\",
  CASE c.relkind WHEN 'r' THEN 'table' WHEN 'v' THEN 'view' WHEN 'm' THEN 'materialized view' WHEN 'i' THEN 'index' WHEN 'S' THEN 'sequence' WHEN 's' THEN 'special' WHEN 'f' THEN 'foreign table' WHEN 'p' THEN 'table' WHEN 'I' THEN 'index' END as \"Type\",
  pg_catalog.pg_get_userbyid(c.relowner) as \"Owner\"
FROM pg_catalog.pg_class c
     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind IN ('r','p','v','m','S','f','')
      AND n.nspname <> 'pg_catalog'
      AND n.nspname <> 'information_schema'
      AND n.nspname !~ '^pg_toast'
  AND pg_catalog.pg_table_is_visible(c.oid)
ORDER BY 1,2;
";

        self.run_query(describe_sql)
    }

    pub fn run_query(&mut self, sql: &str) -> PogoResult {
        let records = self.client.query(sql, &[]).expect("Error querying DB");

        let mut rows = vec![];

        let header = self.get_header(&records[0]);

        for row in records {
            let mut row_result = vec![];

            for i in 0..row.len() {
                let column_type = row.columns().get(i).unwrap().type_();
                let value = self.render_value(column_type, &row, i);

                row_result.push(value);
            }
            rows.push(row_result)
        }

        PogoResult {
            header,
            rows,
        }
    }

    fn render_value(&self, column_type: &Type, row: &Row, index: usize) -> String {
        let value = match column_type {
            &Type::VARCHAR | &Type::TEXT => self.parse_value::<&str>(row, index),
            &Type::UUID => self.parse_value::<Uuid>(&row, index),
            &Type::INT4 => self.parse_value::<i32>(&row, index),
            col_type => {
            // format!("ERR - Couldn't parse type: `{}`", col_type).to_string()
               self.parse_value::<&str>(row, index)
            }
        };

        value
    }

    fn parse_value<'a, T>(&self, row: &'a Row, index: usize) -> String
        where T: FromSql<'a> + Display {
        let val: Option<T> = row.try_get(index).unwrap_or(Option::None);

        match val {
            None => " ".to_string(),
            Some(val) => val.to_string()
        }
    }

    fn get_header(&self, row: &Row) -> Vec<String> {
        let mut result = vec![];
        for column in row.columns() {
            let col = format!("{} ({})", column.name(), column.type_());
            result.push(col)
        }

        result
    }
}