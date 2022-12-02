use anyhow::Result;
use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::datasource::{TableProvider, TableType};
use datafusion::execution::context::{SessionState, TaskContext};
use datafusion::logical_expr::Expr;
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::{
    project_schema, ExecutionPlan, Partitioning, SendableRecordBatchStream, Statistics,
};
use reqwest::{Client, Method, Request, Response, Url};
use serde::Deserialize;
use serde_json::Value;
use std::any::Any;
use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use thiserror::Error;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Record {
    pub id: String,
    pub created_time: String,
    pub fields: HashMap<String, Value>,
}

#[derive(Deserialize)]
pub struct Records {
    pub records: Vec<Record>,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct AirtableField {
    description: String,
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

#[derive(Deserialize)]
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

pub struct Airtable {
    table: Table<String>,
    base: String,
    api_key: String,
    schema_ref: SchemaRef,
    client: AirtableClient,
}

impl Airtable {
    pub async fn new(table_name: String, base: String, api_key: String) -> Result<Self> {
        let reqwest_client = reqwest::Client::new();

        let client = AirtableClient {
            client: reqwest_client,
            api_key: api_key.clone(),
        };

        let url = Url::parse(&format!(
            "https://api.airtable.com/v0/meta/bases/{}/tables",
            base
        ))?;

        let request = client.get_request(url).build()?;
        let tables = client.execute(request).await?.json::<Tables>().await?;

        let table = tables
            .tables
            .into_iter()
            .find(|t| t.name.as_ref() == Some(&table_name))
            .map(|table| table.with_name(table_name))
            .unwrap();

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
        projection: &Option<Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        todo!()
    }
}

#[derive(Debug)]
struct AirtableScan {
    projected_schema: SchemaRef,
    base: String,
    table: Table<String>,
    client: AirtableClient,
}

impl AirtableScan {
    async fn records(&self, page_size: u16, offset: u16) -> Result<Records> {
        let url = Url::parse(&format!(
            "https://api.airtable.com/v0/{}/{}",
            self.base, self.table.name
        ))?;

        let request = self
            .client
            .get_request(url)
            .query(&[
                ("offset", offset.to_string()),
                ("page_size", page_size.to_string()),
            ])
            .build()?;

        Ok(self
            .client
            .execute(request)
            .await?
            .json::<Records>()
            .await?)
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
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion::common::Result<SendableRecordBatchStream> {
        todo!()
    }

    fn statistics(&self) -> Statistics {
        todo!()
    }
}
