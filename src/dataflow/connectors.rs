pub(crate) mod airtable {
    use anyhow::Result;
    use reqwest::{Method, Url};
    use serde::Deserialize;
    use serde_json::Value;
    use std::collections::HashMap;

    #[derive(Deserialize, Debug)]
    #[serde(rename_all = "camelCase")]
    pub(crate) struct Record {
        pub id: String,
        pub created_time: String,
        pub fields: HashMap<String, Value>,
    }

    #[derive(Deserialize, Debug)]
    pub(crate) struct Records {
        pub(crate) records: Vec<Record>,
    }

    pub(crate) async fn records(
        table: &str,
        base: &str,
        api_key: &str,
        page_size: u16,
        offset: u16,
    ) -> Result<Records> {
        let client = reqwest::Client::new();

        let url = Url::parse(&format!("https://api.airtable.com/v0/{}/{}", base, table))?;

        let request = client
            .request(Method::GET, url)
            .bearer_auth(api_key)
            .query(&[
                ("offset", offset.to_string()),
                ("page_size", page_size.to_string()),
            ])
            .build()?;

        Ok(client.execute(request).await?.json::<Records>().await?)
    }
}
