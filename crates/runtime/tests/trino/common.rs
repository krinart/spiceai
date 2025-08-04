/*
Copyright 2024-2025 The Spice.ai OSS Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

use crate::{
    container_registry,
    docker::{ContainerRunnerBuilder, RunningContainer},
};
use bollard::secret::HealthConfig;
use reqwest::Client;
use reqwest::header::HeaderMap;
use serde_json::Value;
use spicepod::{
    acceleration::Acceleration, component::dataset::Dataset, param::Params as DatasetParams,
};
use std::collections::HashMap;
use std::time::Duration;
use tokio::time::sleep;
use tracing::instrument;

const TRINO_DOCKER_CONTAINER: &str = "runtime-integration-test-trino";

pub fn make_trino_dataset(path: &str, name: &str, port: u16, accelerated: bool) -> Dataset {
    let mut dataset = Dataset::new(format!("trino:{path}"), name.to_string());
    let params = HashMap::from([
        ("trino_host".to_string(), "localhost".to_string()),
        ("trino_port".to_string(), port.to_string()),
        ("trino_catalog".to_string(), "memory".to_string()),
        ("trino_schema".to_string(), "default".to_string()),
        ("trino_user".to_string(), "test".to_string()),
        ("trino_ssl".to_string(), "false".to_string()),
    ]);
    dataset.params = Some(DatasetParams::from_string_map(params));
    if accelerated {
        dataset.acceleration = Some(Acceleration::default());
    }
    dataset
}

#[instrument]
pub async fn start_trino_docker_container(
    port: u16,
) -> Result<RunningContainer<'static>, anyhow::Error> {
    let container_name = format!("{TRINO_DOCKER_CONTAINER}-{port}");
    let container_name: &'static str = Box::leak(container_name.into_boxed_str());

    let running_container = ContainerRunnerBuilder::new(container_name)
        .image("trinodb/trino:latest".to_string())
        .add_port_binding(8080, port)
        .healthcheck(HealthConfig {
            test: Some(vec![
                "CMD-SHELL".to_string(),
                "curl -f http://localhost:8080/v1/info || exit 1".to_string(),
            ]),
            interval: Some(5_000_000_000), // 5 seconds
            timeout: Some(10_000_000_000), // 10 seconds
            retries: Some(10),
            start_period: Some(30_000_000_000),  // 30 seconds
            start_interval: Some(2_000_000_000), // 2 seconds
        })
        .build()?
        .run(None)
        .await?;

    // Give Trino some extra time to fully initialize
    tokio::time::sleep(std::time::Duration::from_millis(10000)).await;
    Ok(running_container)
}

pub struct TrinoClient {
    reqwest_client: Client,
    base_url: String,
}

impl TrinoClient {
    pub fn new(port: u16) -> Self {
        let mut headers = HeaderMap::new();
        headers.insert("X-Trino-Catalog", "memory".parse().unwrap());
        headers.insert("X-Trino-Schema", "default".parse().unwrap());
        headers.insert("X-Trino-User", "test".parse().unwrap());

        let client = Client::builder().default_headers(headers).build().unwrap();

        Self {
            reqwest_client: client,
            base_url: format!("http://localhost:{port}"),
        }
    }

    async fn execute(&self, query: &str) -> Result<Vec<Vec<serde_json::Value>>, anyhow::Error> {
        // Submit the query
        let url = format!("{}/v1/statement", self.base_url);
        let response = self
            .reqwest_client
            .post(&url)
            .body(query.to_string())
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(anyhow::anyhow!(
                "Failed to submit query: HTTP {}: {}",
                response.status(),
                response.text().await?
            ));
        }

        let mut result: Value = response.json().await?;
        let mut all_data = Vec::new();

        loop {
            let state = result["stats"]["state"].as_str().unwrap_or("");

            // Extract data rows
            if let Some(data) = result.get("data").and_then(|d| d.as_array()) {
                for row in data {
                    if let Some(row_array) = row.as_array() {
                        all_data.push(row_array.clone());
                    }
                }
            }

            // Check if query is finished
            if state == "FINISHED" {
                break;
            }

            if let Some(next_uri) = result.get("nextUri").and_then(|u| u.as_str()) {
                // Wait before polling
                sleep(Duration::from_millis(50)).await;

                let response = self.reqwest_client.clone().get(next_uri).send().await?;

                if !response.status().is_success() {
                    let status_code = response.status().as_u16();
                    let message = response.text().await.unwrap_or_default();
                    return Err(anyhow::anyhow!(
                        "Failed to submit query: HTTP {}: {}",
                        status_code,
                        message
                    ));
                }

                result = response.json().await?;
            } else {
                if state != "FINISHED" {
                    // No next URI but query not finished - this shouldn't happen
                    return Err(anyhow::anyhow!(
                        "Query not finished but no nextUri provided. State: {}",
                        state
                    ));
                }
                break;
            }
        }

        Ok(all_data)
    }

    // Convenience method for DDL queries that don't return data
    pub async fn execute_ddl(&self, query: &str) -> Result<(), anyhow::Error> {
        self.execute(query).await?;
        Ok(())
    }

    pub async fn execute_query(&self, query: &str) -> Result<(), anyhow::Error> {
        self.execute(query).await?;
        Ok(())
    }
}

pub(super) async fn get_trino_client(port: u16) -> Result<TrinoClient, anyhow::Error> {
    let client = TrinoClient::new(port);

    // Test connection and setup memory catalog
    let mut retries = 15;
    let mut last_err = None;
    while retries > 0 {
        match client.execute("SELECT 1").await {
            Ok(_) => {
                println!("Trino client connected successfully");
                return Ok(client);
            }
            Err(e) => {
                last_err = Some(e);
                tokio::time::sleep(std::time::Duration::from_millis(1000)).await;
                retries -= 1;
                println!("Trino connection test failed, retrying...");
            }
        }
    }

    if let Some(err) = last_err {
        return Err(anyhow::anyhow!(
            "Failed to connect to Trino after retries: {}",
            err
        ));
    }

    Ok(client)
}
