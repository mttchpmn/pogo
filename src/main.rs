use clap::{App, AppSettings, Arg};
use prettytable::{Cell, format, Row, Table};

use pogo::Pogo;

mod pogo;

fn main() {
    let app = App::new("pogo")
        .setting(AppSettings::ArgRequiredElseHelp)
        .version("0.1")
        .about("Utility for executing common operations with a PSQL database")
        .subcommand(App::new("describe").arg(Arg::new("table_name")))
        .subcommand(App::new("run").arg(Arg::new("pogo_name").required(true)));
    let matches = &app.get_matches();

    let connection_string = "postgresql://postgres:postgres@localhost/cdd";
    let mut pogo = Pogo::new(connection_string);

    match matches.subcommand() {
        Some(("describe", sub_matches)) => {
            let table_name = sub_matches.value_of("table_name");
            let result = pogo.describe(table_name);

            let table = make_table(&result.header, &result.rows);
            table.printstd();
        },
        Some(("run", _sub_matches)) => println!("Run command not yet supported"),
        None => {}
        _ => unreachable!()
    }
}

fn make_table(header: &Vec<String>, rows: &Vec<Vec<String>>) -> Table {
    let mut table = Table::new();

    let format = format::FormatBuilder::new()
        .column_separator('|')
        .borders('|')
        .separators(&[format::LinePosition::Top,
            format::LinePosition::Bottom],
                    format::LineSeparator::new('-', '+', '+', '+'))
        .padding(1, 1)
        .build();
    table.set_format(format);

    let header = make_row(header);
    table.add_row(header);

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
