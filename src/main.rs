use prettytable::{Cell, format, Row, Table};

use pogo::Pogo;

mod pogo;

fn main() {
    let connection_string = "postgresql://postgres:postgres@localhost/cdd";
    let mut pogo = Pogo::new(connection_string);
    let result = pogo.describe();
    let table = make_table(&result.header, &result.rows);

    table.printstd();
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
