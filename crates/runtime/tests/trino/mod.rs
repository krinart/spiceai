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

use std::sync::Arc;
use std::time::SystemTime;

use common::{get_trino_client, make_trino_dataset, start_trino_docker_container};

use chrono::{DateTime, Utc};
use util::{RetryError, fibonacci_backoff::FibonacciBackoffBuilder, retry};

use crate::init_tracing;
use crate::utils::{runtime_ready_check, test_request_context};

pub mod common;

use super::*;
use app::AppBuilder;
use runtime::Runtime;
use tracing::instrument;

const TRINO_PORT1: u16 = 8080;

#[instrument]
async fn init_trino_db(port: u16) -> Result<(), anyhow::Error> {
    tracing::debug!("INIT DB: test");
    let client = get_trino_client(port).await?;

    // Create the test table
    tracing::debug!("CREATE TABLE test");
    let create_table_sql = r#"
        CREATE TABLE memory.default.test (
            id bigint,
            col_bit boolean,
            col_tiny tinyint,
            col_short smallint,
            col_long integer,
            col_longlong bigint,
            col_float real,
            col_double double,
            col_timestamp timestamp,
            col_date date,
            col_time time,
            col_blob varbinary,
            col_string varchar,
            col_decimal decimal(10,2),
            col_unsigned_int bigint,
            col_char char(3),
            col_set array(varchar),
            col_json json
        )
    "#;

    // Drop table if exists first
    let _ = client.execute_ddl("DROP TABLE IF EXISTS memory.default.test").await;
    client.execute_ddl(create_table_sql).await?;

    let ts = DateTime::parse_from_rfc3339("2019-01-01T00:00:00Z")?.with_timezone(&Utc);

    // Insert test documents
    let insert_sql = r#"
        INSERT INTO memory.default.test VALUES
        (
            1,
            true,
            1,
            1,
            1,
            1,
            1.1,
            1.1,
            timestamp '2019-01-01 00:00:00',
            date '2019-01-01',
            time '12:34:56',
            to_utf8('blob'),
            'string 🚀😊',
            1.11,
            10,
            'USA',
            array['apple', 'banana'],
            json '{"name": "John", "age": 30, "is_active": true, "balance": 1234.56}'
        ),
        (
            2,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null
        )
    "#;

    client.execute_query(insert_sql).await?;
    Ok(())
}

#[tokio::test]
async fn trino_integration_test() -> Result<(), String> {
    type QueryTests<'a> = Vec<(&'a str, &'a str, Option<Box<ValidateFn>>)>;
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let running_container =
                start_trino_docker_container(TRINO_PORT1)
                    .await
                    .map_err(|e| {
                        tracing::error!("start_trino_docker_container: {e}");
                        e.to_string()
                    })?;
            tracing::debug!("Container started");
            let retry_strategy = FibonacciBackoffBuilder::new().max_retries(Some(10)).build();
            retry(retry_strategy, || async {
                init_trino_db(TRINO_PORT1)
                    .await
                    .map_err(|e| {
                        tracing::error!("Failed transiently to initialize Trino database: {e}");
                        RetryError::transient(e)
                    })
            })
                .await
                .map_err(|e| {
                    tracing::error!("Failed to initialize Trino database: {e}");
                    e.to_string()
                })?;
            let app = AppBuilder::new("trino_integration_test")
                .with_dataset(make_trino_dataset("memory.default.test", "test", TRINO_PORT1, false))
                .build();

            let mut rt = Runtime::builder()
                .with_app(app)
                .with_datafusion_configuration_fn(configure_test_datafusion)
                .build()
                .await;

            let cloned_rt = Arc::new(rt.clone());

            // Set a timeout for the test
            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(60)) => {
                    return Err("Timed out waiting for datasets to load".to_string());
                }
                () = cloned_rt.load_components() => {}
            }

            let queries: QueryTests = vec![(
                "SELECT id, col_bit, col_tiny, col_short, col_long, col_longlong, col_float, col_double, col_timestamp, col_date, col_time, col_blob, col_string, col_decimal, col_unsigned_int, col_char, col_set, col_json FROM test",
                "select",
                Some(Box::new(|result_batches| {
                    for batch in &result_batches {
                        assert_eq!(batch.num_columns(), 18, "num_cols: {}", batch.num_columns());
                        assert_eq!(batch.num_rows(), 2, "num_rows: {}", batch.num_rows());
                    }

                    // snapshot the values of the results
                    let results = arrow::util::pretty::pretty_format_batches(&result_batches)
                        .expect("should pretty print result batch");
                    insta::with_settings!({
                        description => format!("Trino Integration Test Results"),
                        omit_expression => true,
                        snapshot_path => "../snapshots"
                    }, {
                        insta::assert_snapshot!("trino_integration_test", results);
                    });
                })),
            )];

            for (query, snapshot_suffix, validate_result) in queries {
                run_query_and_check_results(
                    &mut rt,
                    &format!("trino_integration_test_{snapshot_suffix}"),
                    query,
                    false, // can't snapshot this plan
                    validate_result,
                )
                    .await?;
            }

            running_container.remove().await.map_err(|e| {
                tracing::error!("running_container.remove: {e}");
                e.to_string()
            })?;

            Ok(())
        })
        .await
}