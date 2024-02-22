extern crate csv;
extern crate rusqlite;
extern crate serde;

use csv::Writer;
use rusqlite::{Connection, Result, Row};
use serde::Serialize;
use std::error::Error;
use std::fs::File;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Serialize)]
struct Content {
    revision_id: u64,
    content: String,
}

fn read_content_from_row(row: &Row) -> Result<Content> {
    Ok(Content {
        revision_id: row.get(0)?,
        content: row.get(1)?,
    })
}

fn export_table_to_csv(
    db_path: &str,
    table_name: &str,
    csv_path: &str,
) -> Result<(), Box<dyn Error>> {
    let conn = Connection::open(db_path)?;

    let mut stmt = conn.prepare(&format!("SELECT * FROM {}", table_name))?;
    let content_iter = stmt.query_map([], read_content_from_row)?;

    let mut wtr = Writer::from_path(csv_path)?;

    for content in content_iter {
        let content = content?;
        wtr.serialize(content)?;
    }

    wtr.flush()?;
    Ok(())
}

fn main() -> Result<(), Box<dyn Error>> {
    export_table_to_csv("revisions.db", "content", "content.csv")
}
