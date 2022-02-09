use postgres::{Client, NoTls};


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

    pub fn describe(&mut self) -> PogoResult {
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
        let rows = self.run_query(describe_sql);

        PogoResult {
            header: vec!["SCHEMA".to_string(), "TABLE NAME".to_string(), "TYPE".to_string()],
            rows,
        }
    }

    fn run_query(&mut self, sql: &str) -> Vec<Vec<String>> {
        let rows = self.client.query(sql, &[]).expect("Error querying DB");

        let mut result = vec![];

        for row in rows {
            let mut row_result = vec![];

            let foo: &str = row.get(1);
            println!("{}", foo);

            for i in 0..row.len() {
                let value: &str = row.get(i);
                row_result.push(String::from(value));
            }
            result.push(row_result)
        }

        result
    }
}