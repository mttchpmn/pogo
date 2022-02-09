use postgres::{Client, NoTls, Row};

use crate::Loader;

pub struct PogoResult {
    pub header: Vec<String>,
    pub rows: Vec<Vec<String>>,
}

pub struct Pogo {
    client: Client,
}

impl Pogo {
    pub fn new(connection_string: &str) -> Self {
        let client = Client::connect(connection_string, NoTls).expect("Error connecting to DB");

        Pogo { client }
    }

    pub fn describe(&mut self, table_name: Option<&str>) -> PogoResult {
        let result = match table_name {
            Some(table_name) => self.describe_table(table_name),
            None => self.describe_database()
        };

        result
    }

    pub fn run(&mut self, operation_name: &str) -> PogoResult {
        let operation = Loader::get_operation(operation_name);

        self.run_query(&operation.command)
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

    fn run_query(&mut self, sql: &str) -> PogoResult {
        let records = self.client.query(sql, &[]).expect("Error querying DB");

        let mut rows = vec![];

        let header = get_header(&records[0]);

        for row in records {
            let mut row_result = vec![];


            for i in 0..row.len() {
                let value: &str = row.get(i);
                row_result.push(String::from(value));
            }
            rows.push(row_result)
        }

        PogoResult {
            header,
            rows,
        }
    }
}

fn get_header(row: &Row) -> Vec<String> {
    let mut result = vec![];
    for column in row.columns() {
        let col = format!("{} ({})", column.name(), column.type_());
        result.push(col)
    }

    result
}