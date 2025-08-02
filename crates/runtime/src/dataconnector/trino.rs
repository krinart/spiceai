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

use crate::component::dataset::Dataset;
use async_trait::async_trait;
use data_components::Read;
use datafusion::datasource::TableProvider;
use datafusion_table_providers::trino::TrinoTableFactory;
use datafusion_table_providers::sql::db_connection_pool::trinodbpool::{Error as TrinoError, TrinoConnectionPool};
use snafu::prelude::*;
use std::any::Any;
use std::convert::Into;
use std::future::Future;
use std::pin::Pin;
use std::string::ToString;
use std::sync::Arc;

use super::{
    ConnectorComponent, ConnectorParams, DataConnector, DataConnectorError, DataConnectorFactory,
    ParameterSpec,
};

#[derive(Debug, Snafu)]
pub enum Error {
    // #[snafu(display("Unable to create Trino connection pool: {source}"))]
    // UnableToCreateTrinoConnectionPool { source: TrinoError },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub struct Trino {
    trino_factory: TrinoTableFactory,
}

impl std::fmt::Debug for Trino {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Trino").finish_non_exhaustive()
    }
}

#[derive(Default, Copy, Clone)]
pub struct TrinoFactory {}

impl TrinoFactory {
    #[must_use]
    pub fn new() -> Self {
        Self {}
    }

    #[must_use]
    pub fn new_arc() -> Arc<dyn DataConnectorFactory> {
        Arc::new(Self {}) as Arc<dyn DataConnectorFactory>
    }
}

const PARAMETERS: &[ParameterSpec] = &[
    ParameterSpec::component("url").secret(),
    ParameterSpec::component("host"),
    ParameterSpec::component("port"),
    ParameterSpec::component("ssl").secret(),
    ParameterSpec::component("catalog").secret(),
    ParameterSpec::component("schema").secret(),
    ParameterSpec::component("user").secret(),
    ParameterSpec::component("password").secret(),
    ParameterSpec::component("timeout_ms").secret(),
    ParameterSpec::component("ssl_verification").secret(),
    ParameterSpec::component("identity_pem_path").secret(),
    ParameterSpec::component("bearer_token").secret(),
    ParameterSpec::component("poll_wait_time_ms").secret(),
];

impl DataConnectorFactory for TrinoFactory {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn create(
        &self,
        mut params: ConnectorParams,
    ) -> Pin<Box<dyn Future<Output = super::NewDataConnectorResult> + Send>> {
        Box::pin(async move {
            let pool = match TrinoConnectionPool::new(params.parameters.to_secret_map()).await {
                Ok(pool) => Arc::new(pool),
                Err(error) => match error {
                    TrinoError::AuthenticationFailedError => {
                        return Err(
                            DataConnectorError::UnableToConnectInvalidUsernameOrPassword {
                                dataconnector: "trino".to_string(),
                                connector_component: params.component.clone(),
                            }
                                .into(),
                        );
                    }

                    _ => {
                        return Err(DataConnectorError::UnableToConnectInternal {
                            dataconnector: "trino".to_string(),
                            connector_component: params.component.clone(),
                            source: Box::new(error),
                        }
                            .into());
                    }
                },
            };

            let trino_factory = TrinoTableFactory::new(pool);

            Ok(Arc::new(Trino { trino_factory }) as Arc<dyn DataConnector>)
        })
    }

    fn prefix(&self) -> &'static str {
        "trino"
    }

    fn parameters(&self) -> &'static [ParameterSpec] {
        PARAMETERS
    }
}

#[async_trait]
impl DataConnector for Trino {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn read_provider(
        &self,
        dataset: &Dataset,
    ) -> super::DataConnectorResult<Arc<dyn TableProvider>> {
        Ok(Read::table_provider(
            &self.trino_factory,
            dataset.path().into(),
            dataset.schema(),
        )
            .await
            .context(super::UnableToGetReadProviderSnafu {
                dataconnector: "trino",
                connector_component: ConnectorComponent::from(dataset),
            })?)
    }
}