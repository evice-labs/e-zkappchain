// crates/engine-core/src/wal.rs

use anyhow::Result;
use bincode::config;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Write};

use crate::LogEntry;

pub struct WalHandler {
    writer: BufWriter<File>,
}

impl WalHandler {
    pub fn new(path: &str) -> Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open(path)?;

        Ok(Self {
            writer: BufWriter::new(file),
        })
    }

    pub fn write_entry(&mut self, entry: &LogEntry) -> Result<()> {
        bincode::serde::encode_into_std_write(entry, &mut self.writer, config::standard())?;
        self.writer.flush()?;
        Ok(())
    }
    pub fn read_all(path: &str) -> Result<Vec<LogEntry>> {
        let file = OpenOptions::new().read(true).open(path);

        if file.is_err() {
            return Ok(Vec::new());
        }

        let file = file.unwrap();
        let mut reader = BufReader::new(file);
        let mut entries = Vec::new();

        loop {
            // decode_from_std_read mengembalikan Result<T, DecodeError>
            // disini menentukan tipe T secara eksplisit atau via inferensi
            let result: Result<LogEntry, _> =
                bincode::serde::decode_from_std_read(&mut reader, config::standard());

            match result {
                Ok(entry) => {
                    entries.push(entry);
                }
                Err(_) => {
                    // Error biasanya berarti EOF (End of File)
                    break;
                }
            }
        }

        Ok(entries)
    }
}
