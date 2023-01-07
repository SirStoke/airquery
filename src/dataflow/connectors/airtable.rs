use crate::dataflow::connectors::airtable::AirtableColumnBuilder::BoolBuilder;
use anyhow::Result;
use async_trait::async_trait;
use datafusion::arrow::array::{
    ArrayRef, BooleanBuilder, Float64Builder, StringBuilder, UInt8Builder,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::error::ArrowError;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::execution::context::{SessionState, TaskContext};
use datafusion::logical_expr::Expr;
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::{
    project_schema, ExecutionPlan, Partitioning, RecordBatchStream, SendableRecordBatchStream,
    Statistics,
};
use futures::{future, stream, Stream};
use indexmap::IndexMap;
use reqwest::{Client, Method, Request, Response, Url};
use serde::de::Unexpected::Float;
use serde::Deserialize;
use serde_json::Value;
use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::ops::DerefMut;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use thiserror::Error;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Record {
    pub id: String,
    pub created_time: String,
    pub fields: HashMap<String, Value>,
}

#[derive(Error, Debug)]
enum AirtableColumnBuilderError {
    #[error("Invalid builder found ({column_type:?} can't be pushed into {builder_type:?})")]
    InvalidBuilder {
        column_type: String,
        builder_type: String,
    },

    #[error("Unsupported column type: {column_type:?})")]
    UnsupportedType { column_type: String },

    #[error("Could not find arrow builder for {0}")]
    FieldNotFound(String),
}

#[derive(Debug)]
enum AirtableColumnBuilder {
    BoolBuilder(BooleanBuilder),
    NumberBuilder(Float64Builder),
    StrBuilder(StringBuilder),

    // Used by all-null columns
    Null(BooleanBuilder),
}

use AirtableColumnBuilder::*;

impl AirtableColumnBuilder {
    fn is_null(&self) -> bool {
        matches!(self, Null(_))
    }

    fn append_value(&mut self, value: &Value) -> Result<(), AirtableColumnBuilderError> {
        match (self, value) {
            (BoolBuilder(builder), Value::Bool(b)) => Ok(builder.append_value(*b)),

            (NumberBuilder(builder), Value::Number(n)) => {
                Ok(builder.append_value(n.as_f64().unwrap()))
            }

            (StrBuilder(builder), Value::String(s)) => Ok(builder.append_value(&s)),

            (Null(builder), _) => Ok(builder.append_null()),

            (builder, Value::Null) => Ok(builder.append_null()),

            (builder, value) => Err(AirtableColumnBuilderError::InvalidBuilder {
                column_type: format!("{:?}", value),
                builder_type: format!("{:?}", builder),
            }),
        }
    }

    fn append_null(&mut self) {
        match self {
            NumberBuilder(builder) => builder.append_null(),
            BoolBuilder(builder) => builder.append_null(),
            StrBuilder(builder) => builder.append_null(),
            Null(builder) => builder.append_null(),
        }
    }

    /// Creates an AirtableColumnBuilder based on the column value. Importantly, it doesn't append
    /// the value itself, leaving that responsibility to the caller
    fn from_value(value: &Value) -> Result<AirtableColumnBuilder, AirtableColumnBuilderError> {
        match value {
            Value::String(_) => Ok(StrBuilder(StringBuilder::new())),

            Value::Bool(_) => Ok(BoolBuilder(BooleanBuilder::new())),

            Value::Number(_) => Ok(NumberBuilder(Float64Builder::new())),

            // The caller _must_ try to search for a non-null value for this column. If they couldn't
            // find one, let's default to a boolbuilder
            Value::Null => Ok(BoolBuilder(BooleanBuilder::new())),

            _ => Err(AirtableColumnBuilderError::UnsupportedType {
                column_type: format!("{:?}", value),
            }),
        }
    }

    fn finish(&mut self) -> ArrayRef {
        match self {
            BoolBuilder(b) => Arc::new(b.finish()),
            StrBuilder(s) => Arc::new(s.finish()),
            NumberBuilder(n) => Arc::new(n.finish()),
            Null(n) => Arc::new(n.finish()),
        }
    }

    fn null() -> AirtableColumnBuilder {
        Null(BooleanBuilder::new())
    }
}

impl Record {
    fn fill_builders<'a, I>(&self, builders: I) -> Result<(), AirtableColumnBuilderError>
    where
        I: Iterator<Item = (&'a String, &'a mut AirtableColumnBuilder)>,
    {
        for (field, builder) in builders {
            if let Some(value) = self.fields.get(field) {
                builder.append_value(value)?;
            } else {
                builder.append_null();
            }
        }

        Ok(())
    }
}

#[derive(Deserialize)]
pub struct Records {
    pub records: Vec<Record>,
}

impl Records {
    fn build_columns(
        &self,
        schema_ref: SchemaRef,
    ) -> Result<IndexMap<String, AirtableColumnBuilder>> {
        let mut builders: IndexMap<String, AirtableColumnBuilder> = IndexMap::new();

        let projected_fields: HashSet<String> = schema_ref
            .fields
            .iter()
            .map(|field| field.name().clone())
            .collect();

        // First, we create the arrow builders
        for record in self.records.iter() {
            for (k, v) in record.fields.iter() {
                let curr_builder = builders.get(k);

                // We should create a builder if either one is missing, or a non-null value was
                // found and we had a Null builder previously indexed
                let should_insert_builder = (curr_builder.is_none()
                    || (matches!(curr_builder, Some(b) if b.is_null()) && v != &Value::Null))
                    && projected_fields.contains(k);

                if should_insert_builder {
                    match AirtableColumnBuilder::from_value(v) {
                        Ok(builder) => {
                            builders.insert(k.clone(), builder);
                        }

                        Err(err) => eprintln!("{:?}", err),
                    }
                }
            }
        }

        let keys: HashSet<String> = builders.keys().cloned().collect();

        // At this point, if a field is present in the projected schema but it isn't in the builders
        // collected until now, it means that we couldn't find the field in any record, and it should
        // be set as null
        for missing_builder in projected_fields.difference(&keys) {
            builders.insert(missing_builder.to_string(), AirtableColumnBuilder::null());
        }

        // Then, we fill the builders with the actual records. We do this in two separate steps
        // to make sure we have all columns in all records, and that we see a non-null column
        // if there is one (to make out its type)
        for record in self.records.iter() {
            record.fill_builders(builders.iter_mut())?;
        }

        dbg!(&builders);

        Ok(builders)
    }
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct AirtableField {
    description: Option<String>,
    id: String,
    name: String,
    #[serde(rename = "type")]
    type_: String,
}

#[derive(Deserialize, Debug)]
struct Table<N> {
    fields: Vec<AirtableField>,
    name: N,
}

impl<N> Table<N> {
    fn with_name<U>(self, name: U) -> Table<U> {
        Table {
            fields: self.fields,
            name,
        }
    }
}

#[derive(Deserialize, Debug)]
struct Tables {
    tables: Vec<Table<Option<String>>>,
}

#[derive(Debug)]
struct AirtableClient {
    client: Client,
    api_key: String,
}

impl AirtableClient {
    fn get_request(&self, url: Url) -> reqwest::RequestBuilder {
        self.client
            .request(Method::GET, url)
            .bearer_auth(self.api_key.clone())
    }

    async fn execute(&self, request: Request) -> Result<Response> {
        self.client.execute(request).await.map_err(|err| err.into())
    }
}

#[derive(Debug, Clone)]
pub struct Airtable {
    table: Arc<Table<String>>,
    base: String,
    api_key: String,
    schema_ref: SchemaRef,
    client: Arc<AirtableClient>,
}

impl Airtable {
    pub async fn new(table_name: String, base: String, api_key: String) -> Result<Self> {
        let reqwest_client = reqwest::Client::new();

        let client = Arc::new(AirtableClient {
            client: reqwest_client,
            api_key: api_key.clone(),
        });

        let url = Url::parse(&format!(
            "https://api.airtable.com/v0/meta/bases/{}/tables",
            base
        ))?;

        let request = client.get_request(url).build()?;
        let tables = client.execute(request).await?.json::<Tables>().await?;

        let table = Arc::new(
            tables
                .tables
                .into_iter()
                .find(|t| t.name.as_ref() == Some(&table_name))
                .map(|table| table.with_name(table_name))
                .unwrap(),
        );

        let schema_ref = Self::build_schema_ref(&table);

        Ok(Self {
            table,
            base,
            api_key,
            schema_ref,
            client,
        })
    }

    fn build_schema_ref(table: &Table<String>) -> SchemaRef {
        let fields = table.fields.iter().filter_map(|field| {
            let type_ = match field.type_.as_str() {
                "singleLineText" => Some(DataType::Utf8),
                any => {
                    println!("Unknown type: {}", any);

                    None
                }
            };

            if let Some(type_) = type_ {
                Some(Field::new(&field.name, type_, true))
            } else {
                None
            }
        });

        SchemaRef::new(Schema::new(fields.collect()))
    }

    async fn records(&self, page_size: u16, offset: u16) -> Result<Records> {
        let url = Url::parse(&format!(
            "https://api.airtable.com/v0/{}/{}?offset={}&page_size={}",
            self.base,
            self.table.name,
            offset.to_string(),
            page_size.to_string()
        ))?;

        let request = self.client.get_request(url).build()?;

        Ok(self
            .client
            .execute(request)
            .await?
            .json::<Records>()
            .await?)
    }
}

#[async_trait]
impl TableProvider for Airtable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema_ref.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        ctx: &SessionState,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let projected_schema = if let Some(projection) = projection {
            Arc::new(self.schema().project(projection)?)
        } else {
            self.schema()
        };

        Ok(Arc::new(AirtableScan {
            airtable: self.clone(),
            projected_schema,
        }))
    }
}

#[derive(Debug)]
struct AirtableScan {
    airtable: Airtable,
    projected_schema: SchemaRef,
}

struct AirtableStream<S>
where
    S: Stream<Item = Result<RecordBatch, ArrowError>>,
{
    stream: Pin<Box<S>>,
    schema: SchemaRef,
}

impl<S> Stream for AirtableStream<S>
where
    S: Stream<Item = Result<RecordBatch, ArrowError>>,
{
    type Item = Result<RecordBatch, ArrowError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let stream = self.stream.as_mut();

        stream.poll_next(cx)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.stream.size_hint()
    }
}

impl<S> RecordBatchStream for AirtableStream<S>
where
    S: Stream<Item = Result<RecordBatch, ArrowError>>,
{
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl ExecutionPlan for AirtableScan {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.projected_schema.clone()
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(1)
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _: usize,
        _: Arc<TaskContext>,
    ) -> datafusion::common::Result<SendableRecordBatchStream> {
        let airtable = self.airtable.clone();

        let batch_stream = stream::unfold(
            (airtable, self.projected_schema.clone(), 0),
            move |(airtable, schema_ref, page)| async move {
                let page_size = 500;
                let offset = page * page_size;

                match airtable.records(page_size, offset).await {
                    Ok(records) if !records.records.is_empty() => {
                        let columns: Vec<ArrayRef> = records
                            .build_columns(schema_ref.clone())
                            .unwrap()
                            .values_mut()
                            .map(|b| b.finish())
                            .collect();

                        dbg!(&columns);

                        let batch = RecordBatch::try_new(schema_ref.clone(), columns);

                        dbg!(&batch);

                        Some((batch, (airtable, schema_ref, page + 1)))
                    }
                    Ok(_) => None,
                    Err(_) => None,
                }
            },
        );

        Ok(Box::pin(AirtableStream {
            stream: Box::pin(batch_stream),
            schema: self.projected_schema.clone(),
        }))
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }
}
