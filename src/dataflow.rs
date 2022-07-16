use async_trait::async_trait;
use serde_json::Value;
use std::collections::VecDeque;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::Mutex;

pub(crate) mod connectors;

struct Table(String);

struct Column {
    name: String,
    table: Table,
}

impl Column {
    fn new(name: String, table: String) -> Column {
        Column {
            name,
            table: Table(table),
        }
    }
}

impl PartialEq for Column {
    fn eq(&self, other: &Self) -> bool {
        self.table.0 == other.table.0 && self.name == other.name
    }
}

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

#[derive(Error, Debug)]
enum PropertiesError {
    #[error("node internally failed to provide properties")]
    Internal(#[from] anyhow::Error),
    #[error("couldn't find properties for table `{0}`")]
    EmptyProperties(String),
    #[error("node wasn't open")]
    NotOpen,
}

/// Represents some data found in a Tuple - e.g. a number or a text
#[derive(Clone)]
enum Datum {
    Number(f64),
    Text(String),
}

struct Tuple {
    columns: Vec<Column>,
    data: Vec<Datum>,
}

impl Tuple {
    fn new(columns: Vec<Column>, data: Vec<Datum>) -> Tuple {
        Tuple { columns, data }
    }

    fn with_capacity(size: usize) -> Tuple {
        Tuple {
            columns: Vec::with_capacity(size),
            data: Vec::with_capacity(size),
        }
    }
}

/// Represents the physical properties describing the tuples emitted by the Node.
/// e.g. columns, sorting, etc.
#[derive(Clone)]
struct Properties<'a> {
    sort_by: Option<&'a Vec<String>>,
    columns: &'a Vec<Column>,
}

/// A Data Flow Node
///
/// This is the classic Volcano interface:
///     - #open() prepares the necessary resources (e.g. opens files)
///     - #next() optionally emits a tuple
///     - #close() closes the resources down (e.g. closes files)
///     - #properties() returns the physical properties of the tuples emitted
///
/// #open() must be called before #next(), which must be called before #close()
#[async_trait]
trait Node {
    async fn open(&mut self) -> Result<(), OpenError>;
    async fn next(&mut self) -> Result<Option<Tuple>, NextError>;
    async fn close(&mut self) -> Result<(), CloseError>;

    async fn properties(&mut self) -> Result<Properties, PropertiesError>;
}

const BATCH_SIZE: u16 = 100;

/// A Data Flow node that fetches rows from an Airtable table
///
/// It internally buffers up to [BATCH_SIZE] rows
struct AirtableScanNode {
    table: String,
    base: String,
    api_key: String,
    buffer: VecDeque<Tuple>,
    curr_offset: u16,
}

impl AirtableScanNode {
    /// Loads a batch of rows from the table inside an internal buffer, where they will eventually be
    /// dequeued downstream
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
                    column_names.push(Column::new(column.clone(), self.table.clone()));
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

        Ok(self.buffer.pop_back())
    }

    async fn close(&mut self) -> Result<(), CloseError> {
        Ok(())
    }

    async fn properties(&mut self) -> Result<Properties, PropertiesError> {
        if self.buffer.is_empty() {
            self.load_buffer()
                .await
                .map_err(|err| -> PropertiesError { err.into() })?;
        }

        if self.buffer.is_empty() {
            Err(PropertiesError::EmptyProperties(self.table.clone()))?;
        }

        Ok(Properties {
            sort_by: None,
            columns: &self.buffer[0].columns,
        })
    }
}

struct ProjectNode<'a> {
    flow: &'a mut (dyn Node + Send),
    projection: Vec<Column>,
    properties: Option<Properties<'a>>,
}

#[async_trait]
impl<'a> Node for ProjectNode<'a> {
    async fn open(&mut self) -> Result<(), OpenError> {
        self.flow.open().await?;

        Ok(())
    }

    async fn next(&mut self) -> Result<Option<Tuple>, NextError> {
        if let Some(tuple) = self.flow.next().await? {
            let mut projected = Tuple::with_capacity(self.projection.len());

            tuple.columns.iter().enumerate().for_each(|(i, col)| -> () {
                if self.projection.contains(col) {
                    projected.data.push(tuple.data[i].clone());
                }
            });

            Ok(Some(tuple))
        } else {
            Ok(None)
        }
    }

    async fn close(&mut self) -> Result<(), CloseError> {
        self.flow.close().await
    }

    async fn properties(&mut self) -> Result<Properties, PropertiesError> {
        if let Some(props) = &self.properties {
            Ok(props.clone())
        } else {
            let upstream_props = self.flow.properties().await?;

            Ok(Properties {
                columns: &self.projection,
                sort_by: upstream_props.sort_by,
            })
        }
    }
}
