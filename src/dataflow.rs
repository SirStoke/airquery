use async_trait::async_trait;
use serde_json::Value;
use std::collections::VecDeque;
use thiserror::Error;

pub(crate) mod connectors;

#[derive(Error, Debug)]
enum OpenError {
    #[error("node internally failed to setup")]
    Internal(#[from] anyhow::Error),
}

#[derive(Error, Debug)]
enum NextError {
    #[error("node internally failed to produce a value")]
    Internal(#[from] anyhow::Error),
}

#[derive(Error, Debug)]
enum CloseError {
    #[error("node internally failed to close")]
    Internal(#[from] anyhow::Error),
}

/// Represents some data found in a Tuple - e.g. a number or a text
enum Datum {
    Number(f64),
    Text(String),
}

struct Tuple {
    column_names: Vec<String>,
    data: Vec<Datum>,
}

impl Tuple {
    fn new(column_names: Vec<String>, data: Vec<Datum>) -> Tuple {
        Tuple { column_names, data }
    }
}

/// Represents the physical properties describing the tuples emitted by the Node.
/// e.g. columns, sorting, etc.
struct Properties {
    sort_by: Vec<String>,
    columns: Vec<String>,
}

/// A Data Flow Node
///
/// This is the classic Volcano interface:
///
/// #open() must be called before #next(), which must be called before #close()
#[async_trait]
trait Node {
    async fn open(&mut self) -> Result<(), OpenError>;
    async fn next(&mut self) -> Result<Option<Tuple>, NextError>;
    async fn close(&mut self) -> Result<(), CloseError>;
}

const BATCH_SIZE: u16 = 100;

struct AirtableScanNode {
    table: String,
    base: String,
    api_key: String,
    buffer: VecDeque<Tuple>,
    curr_offset: u16,
}

impl AirtableScanNode {
    async fn load_buffer(&mut self) -> anyhow::Result<()> {
        let records = connectors::airtable::records(
            &self.table,
            &self.base,
            &self.api_key,
            BATCH_SIZE,
            self.curr_offset,
        )
        .await?;

        for record in records.records.iter() {
            let mut column_names = Vec::with_capacity(record.fields.len());
            let mut data = Vec::with_capacity(record.fields.len());

            for (column, value) in record.fields.iter() {
                let datum = match value {
                    Value::Number(number) if number.is_f64() => {
                        Some(Datum::Number(number.as_f64().unwrap()))
                    }
                    Value::String(string) => Some(Datum::Text(string.clone())),
                    _ => None,
                };

                if let Some(datum) = datum {
                    column_names.push(column.clone());
                    data.push(datum);
                }
            }

            self.buffer.push_front(Tuple::new(column_names, data));
        }

        self.curr_offset += BATCH_SIZE;

        Ok(())
    }
}

#[async_trait]
impl Node for AirtableScanNode {
    async fn open(&mut self) -> Result<(), OpenError> {
        AirtableScanNode::load_buffer(self).await?;

        Ok(())
    }

    async fn next(&mut self) -> Result<Option<Tuple>, NextError> {
        if self.buffer.is_empty() {
            AirtableScanNode::load_buffer(self).await?;
        }

        Ok(self.buffer.pop_front())
    }

    async fn close(&mut self) -> Result<(), CloseError> {
        Ok(())
    }
}
