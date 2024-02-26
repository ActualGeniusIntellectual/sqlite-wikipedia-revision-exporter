extern crate csv;
extern crate rusqlite;
extern crate serde;

use csv::Writer;
use env_logger::Builder;
use rusqlite::{Connection, Result, Row};
use serde::Serialize;
use std::error::Error;

use chrono::Local;
use log::{debug, error, info, LevelFilter};
use std::io::Write;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Serialize)]
struct Content {
    revision_id: u64,
    page: String,
    content: String,
}

fn read_content_from_row(row: &Row) -> Result<Content> {
    Ok(Content {
        revision_id: row.get(0)?,
        page: row.get(1)?,
        content: row.get(2)?,
    })
}

fn export_table_to_csv(
    db_path: &str,
    table_name: &str,
    csv_path: &str,
) -> Result<(), Box<dyn Error>> {
    let conn = Connection::open(db_path)?;

    info!("Querying table {}", table_name);
    let mut stmt = conn.prepare(&format!("SELECT * FROM {}", table_name))?;
    let content_iter = stmt.query_map([], read_content_from_row)?;

    // Convert to a list of Content
    let list = content_iter.collect::<Result<Vec<Content>>>()?;

    // Log how many rows we have
    info!("Found {} rows", list.len());

    let mut wtr = Writer::from_path(csv_path)?;

    for content in list {
        let content = content;
        // Debug print the content
        debug!("Storing {:#?}", content);
        wtr.serialize(content)?;
    }

    wtr.flush()?;
    Ok(())
}

fn init() {
    // Initialize logger
    Builder::new()
        .format(|buf, record| {
            writeln!(
                buf,
                "{} [{}] - {}",
                Local::now().format("%Y-%m-%dT%H:%M:%S"),
                record.level(),
                record.args()
            )
        })
        .filter(None, LevelFilter::Info)
        .init();
}

fn main() {
    init();
    info!("Starting...");
    let result = export_table_to_csv("revisions.db", "content", "content.csv");

    match result {
        Ok(_) => info!("Exported successfully!"),
        Err(e) => error!("Error: {}", e),
    }
    info!("Done!");
}
