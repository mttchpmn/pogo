use std::collections::HashMap;
use std::fmt::Display;
use std::path::PathBuf;

use postgres::{Client, NoTls, Row};
use postgres::types::{FromSql, Type};
use prettytable::row;
use uuid::Uuid;

use crate::{Loader, Operation};

#[derive(Debug)]
pub struct PogoResult {
    pub header: Vec<String>,
    pub rows: Vec<Vec<String>>,
}

pub struct Pogo {
    client: Client,
    operations: Vec<Operation>,
}

impl Pogo {
    pub fn new(connection_string: &str) -> Self {
        let client = Client::connect(connection_string, NoTls).expect("Error connecting to DB - is your connection string set properly?");
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

        PogoResult { header, rows }
    }

    pub fn run(&mut self, operation_name: &str) -> PogoResult {
        let operation = self.get_operation(operation_name);

        self.run_query(&operation.command)
    }

    fn get_operation(&self, operation_name: &str) -> Operation {
        // TODO - Remove need to clone in this method
        let Operation { name, command, description } = &self.operations.iter().find(|x| { x.name == operation_name }).expect("Couldn't find operation");

        return Operation { name: name.clone(), command: command.clone(), description: description.clone() };
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

        let mut result = self.run_query(&sql);

        let foreign_keys = self.get_foreign_keys_for_table(table_name);
        let foreign_key_map = self.map_foreign_keys(foreign_keys);

        let result = self.add_foreign_keys(result, foreign_key_map);

        result
    }

    fn map_foreign_keys(&self, foreign_keys: PogoResult) -> HashMap<String, String> {
        let column_name_index = self.get_column_index_by_name("column_name", &foreign_keys.header);
        let foreign_table_name_index = self.get_column_index_by_name("foreign_table_name", &foreign_keys.header);
        let foreign_column_name_index = self.get_column_index_by_name("foreign_column_name", &foreign_keys.header);

        let mut result = HashMap::new();

        for row in foreign_keys.rows {
            let column_name: &str = row.get(column_name_index).unwrap();
            let foreign_table: &str = row.get(foreign_table_name_index).unwrap();
            let foreign_column: &str = row.get(foreign_column_name_index).unwrap();

            let value = format!("{}({})", foreign_table, foreign_column);

            result.insert(column_name.to_string(), value);
        }

        result
    }

    fn get_column_index_by_name(&self, name: &str, columns: &Vec<String>) -> usize {
        let (column_name_index, _) = &columns.iter().enumerate().find(|(i, x)| { x.contains(name) }).unwrap();

        column_name_index.to_owned()
    }

    fn add_foreign_keys(&self, mut rows: PogoResult, keys: HashMap<String, String>) -> PogoResult {
        rows.header.push("REFERENCES".to_string());

        let column_name_index = self.get_column_index_by_name("column_name", &rows.header);

        for row in &mut rows.rows {
            let column_name: &str = row.get(column_name_index.to_owned()).unwrap();

            match keys.get(column_name) {
                Some(foreign_key) => row.push(foreign_key.to_string()),
                None => ()
            }
        }

        rows
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

    pub fn get_foreign_keys_for_table(&mut self, table_name: &str) -> PogoResult {
        let sql = format!("\
SELECT
    tc.table_name,
    kcu.column_name,
    ccu.table_name AS foreign_table_name,
    ccu.column_name AS foreign_column_name
FROM
    information_schema.table_constraints AS tc
    JOIN information_schema.key_column_usage AS kcu
      ON tc.constraint_name = kcu.constraint_name
      AND tc.table_schema = kcu.table_schema
    JOIN information_schema.constraint_column_usage AS ccu
      ON ccu.constraint_name = tc.constraint_name
      AND ccu.table_schema = tc.table_schema
WHERE tc.constraint_type = 'FOREIGN KEY' AND tc.table_name='{}'; ", table_name);
        let result = self.run_query(&sql);

        result
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