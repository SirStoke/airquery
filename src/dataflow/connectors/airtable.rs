use anyhow::Result;
use async_trait::async_trait;
use chrono::{NaiveDate, ParseError};
use datafusion::arrow::array::{
    ArrayRef, Date64Builder, Decimal128Builder, StringBuilder,
};
use datafusion::arrow::datatypes::{
    DataType, Date64Type, Field, Schema, SchemaRef,
};
use datafusion::arrow::error::ArrowError;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::schema::{MemorySchemaProvider, SchemaProvider};
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::Result as DataFusionResult;
use datafusion::execution::context::{SessionState, TaskContext};
use datafusion::logical_expr::Expr;
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::{
    ExecutionPlan, Partitioning, RecordBatchStream, SendableRecordBatchStream,
    Statistics,
};
use futures::{stream, Stream};
use indexmap::IndexMap;
use reqwest::{Client, Method, Request, Response, Url};
use rust_decimal::{Decimal, Error as DecimalError};
use serde::Deserialize;
use serde_json::Value;
use std::any::Any;
use std::collections::HashMap;
use std::pin::Pin;
use std::str::FromStr;
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
    InvalidBuilder { column_type: String, builder_type: String },

    #[error("Failed to parse decimal: {0}")]
    DecimalError(#[from] DecimalError),

    #[error("Failed to parse value: {0}")]
    ParseError(#[from] ParseError),
}

#[derive(Debug)]
enum AirtableColumnBuilder {
    NumberBuilder(Decimal128Builder),
    DateBuilder(Date64Builder),
    StrBuilder(StringBuilder),
}

use AirtableColumnBuilder::*;

impl AirtableColumnBuilder {
    fn append_value(
        &mut self,
        value: &Value,
    ) -> Result<(), AirtableColumnBuilderError> {
        match (self, value) {
            (NumberBuilder(builder), Value::String(n)) => {
                let mut decimal = Decimal::from_str(n)?;

                decimal.rescale(10);

                let unpacked = decimal.unpack();

                let mut number: i128 = ((i128::from(unpacked.hi) << 64)
                    | i128::from(unpacked.mid) << 32)
                    | i128::from(unpacked.lo);

                if unpacked.negative {
                    number = -number;
                }

                Ok(builder.append_value(number))
            }

            (builder, Value::Number(n)) => {
                builder.append_value(&Value::String(n.to_string()))
            }

            (StrBuilder(builder), Value::String(s)) => {
                Ok(builder.append_value(&s))
            }

            (DateBuilder(builder), Value::String(d)) => {
                let d = Date64Type::from_naive_date(NaiveDate::from_str(d)?);

                Ok(builder.append_value(d))
            }

            (builder, Value::Null) => Ok(builder.append_null()),

            (builder, value) => {
                Err(AirtableColumnBuilderError::InvalidBuilder {
                    column_type: format!("{:?}", value),
                    builder_type: format!("{:?}", builder),
                })
            }
        }
    }

    fn append_null(&mut self) {
        match self {
            NumberBuilder(builder) => builder.append_null(),
            DateBuilder(builder) => builder.append_null(),
            StrBuilder(builder) => builder.append_null(),
        }
    }

    fn finish(&mut self) -> ArrayRef {
        match self {
            StrBuilder(s) => Arc::new(s.finish()),
            NumberBuilder(n) => Arc::new(n.finish()),
            DateBuilder(d) => Arc::new(d.finish()),
        }
    }

    fn from_schema_ref(
        schema_ref: SchemaRef,
    ) -> IndexMap<String, AirtableColumnBuilder> {
        let mut builders: IndexMap<String, AirtableColumnBuilder> =
            IndexMap::new();

        for field in schema_ref.fields.iter() {
            let builder = match field.data_type() {
                DataType::Utf8 => Some(StrBuilder(StringBuilder::new())),
                DataType::Decimal128(38, 10) => {
                    Some(NumberBuilder(Decimal128Builder::new()))
                }
                DataType::Date64 => Some(DateBuilder(Date64Builder::new())),
                _ => None,
            };

            if let Some(builder) = builder {
                builders.insert(field.name().clone(), builder);
            }
        }

        builders
    }
}

impl Record {
    fn fill_builders<'a, I>(
        &self,
        builders: I,
    ) -> Result<(), AirtableColumnBuilderError>
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
        let mut builders: IndexMap<String, AirtableColumnBuilder> =
            AirtableColumnBuilder::from_schema_ref(schema_ref.clone());

        for record in self.records.iter() {
            record.fill_builders(builders.iter_mut())?;
        }

        Ok(builders)
    }
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct AirtableField {
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
        Table { fields: self.fields, name }
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
    base: String,
}

impl AirtableClient {
    fn get_request(&self, url: Url) -> reqwest::RequestBuilder {
        self.client.request(Method::GET, url).bearer_auth(self.api_key.clone())
    }

    async fn execute(&self, request: Request) -> Result<Response> {
        self.client.execute(request).await.map_err(|err| err.into())
    }
}

#[derive(Debug, Clone)]
pub struct Airtable {
    tables: Arc<Vec<Table<String>>>,
    schema_refs: Vec<SchemaRef>,
    client: Arc<AirtableClient>,
}

impl Airtable {
    pub async fn new(base: String, api_key: String) -> Result<Self> {
        let reqwest_client = reqwest::Client::new();

        let client = Arc::new(AirtableClient {
            client: reqwest_client,
            api_key: api_key.clone(),
            base: base,
        });

        let url = Url::parse(&format!(
            "https://api.airtable.com/v0/meta/bases/{}/tables",
            &client.base
        ))?;

        let request = client.get_request(url).build()?;
        let res = client.execute(request).await?;

        let tables: Tables = res.json::<Tables>().await?;

        let tables: Vec<Table<String>> = tables
            .tables
            .into_iter()
            .filter_map(|table| {
                let table_name = table.name.clone();

                table_name.map(|name| table.with_name(name))
            })
            .collect();

        let schema_refs = tables
            .iter()
            .map(|table| Self::build_schema_ref(&table))
            .collect::<Vec<SchemaRef>>();

        Ok(Self { tables: Arc::new(tables), schema_refs, client })
    }

    fn build_schema_ref(table: &Table<String>) -> SchemaRef {
        let fields = table.fields.iter().filter_map(|field| {
            let type_ = match field.type_.as_str() {
                "singleLineText" => Some(DataType::Utf8),
                "currency" => Some(DataType::Decimal128(38, 10)),
                "date" => Some(DataType::Date64),
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

    pub fn schema_provider(
        &self,
    ) -> DataFusionResult<Arc<dyn SchemaProvider>> {
        let schema_provider = MemorySchemaProvider::new();

        for (table, schema_ref) in
            self.tables.iter().zip(self.schema_refs.iter())
        {
            schema_provider.register_table(
                table.name.clone(),
                Arc::new(AirtableTableProvider {
                    table_name: table.name.clone(),
                    schema_ref: schema_ref.clone(),
                    client: self.client.clone(),
                }),
            )?;
        }

        Ok(Arc::new(schema_provider))
    }
}

#[derive(Debug, Clone)]
struct AirtableTableProvider {
    table_name: String,
    schema_ref: SchemaRef,
    client: Arc<AirtableClient>,
}

impl AirtableTableProvider {
    async fn records(&self, page_size: u16, offset: u16) -> Result<Records> {
        let url = Url::parse(&format!(
            "https://api.airtable.com/v0/{}/{}?offset={}&page_size={}",
            self.client.base,
            &self.table_name,
            offset.to_string(),
            page_size.to_string()
        ))?;

        let request = self.client.get_request(url).build()?;

        Ok(self.client.execute(request).await?.json::<Records>().await?)
    }
}

#[async_trait]
impl TableProvider for AirtableTableProvider {
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
        _ctx: &SessionState,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let projected_schema = if let Some(projection) = projection {
            Arc::new(self.schema().project(projection)?)
        } else {
            self.schema()
        };

        Ok(Arc::new(AirtableScan {
            airtable_table: self.clone(),
            projected_schema,
        }))
    }
}

#[derive(Debug)]
struct AirtableScan {
    airtable_table: AirtableTableProvider,
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

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
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
        let airtable = self.airtable_table.clone();

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

                        let batch =
                            RecordBatch::try_new(schema_ref.clone(), columns);

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
