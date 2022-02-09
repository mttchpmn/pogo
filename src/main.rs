use clap::{App, AppSettings, Arg};
use prettytable::{Cell, format, Row, Table};

use loader::{Loader, Operation};
use pogo::Pogo;
use crate::pogo::PogoResult;

mod pogo;
mod loader;

fn main() {
    let app = App::new("pogo")
        .setting(AppSettings::ArgRequiredElseHelp)
        .version("0.1")
        .about("Utility for executing common operations with a PSQL database")

        .subcommand(App::new("describe")
            .about("Describe database or table structure")
            .arg(Arg::new("table_name")))

        .subcommand(App::new("list")
            .about("Lists available operations"))

        .subcommand(App::new("run")
            .about("Runs a user defined operation")
            .arg(Arg::new("operation_name")
                .required(true)));
    let matches = &app.get_matches();

    let connection_string = "postgresql://postgres:postgres@localhost/cdd";
    let mut pogo = Pogo::new(connection_string);

    match matches.subcommand() {
        Some(("describe", sub_matches)) => {
            let table_name = sub_matches.value_of("table_name");
            let result = pogo.describe(table_name);

            render_result(&result);
        }
        Some(("run", sub_matches)) => {
            let operation_name = sub_matches.value_of("operation_name").unwrap();
            let result = pogo.run(operation_name);

            render_result(&result);
        },
        Some(("list", _sub_matches)) => println!("List command not yet supported"),
        _ => unreachable!()
    }
}

fn render_result(result: &PogoResult) {
    let table = make_table(&result.header, &result.rows);
    table.printstd();
}

fn make_table(header: &Vec<String>, rows: &Vec<Vec<String>>) -> Table {
    let mut table = Table::new();
    table.set_format(*format::consts::FORMAT_NO_BORDER_LINE_SEPARATOR);

    let header = make_row(header);
    table.set_titles(header);

    for row in rows {
        let row = make_row(row);
        table.add_row(row);
    }

    table
}

fn make_row(row: &Vec<String>) -> Row {
    let cells = row.iter().map(|val| {
        Cell::new(val)
    }).collect();

    Row::new(cells)
}
