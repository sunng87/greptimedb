// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{SystemTime, UNIX_EPOCH};

use common_query::physical_plan::PhysicalPlanRef;
use common_query::prelude::Expr;
use common_recordbatch::error::Result as RecordBatchResult;
use common_recordbatch::{RecordBatch, RecordBatchStream, SendableRecordBatchStream};
use common_time::timestamp::TimeUnit;
use datatypes::prelude::ConcreteDataType;
use datatypes::schema::{ColumnSchema, SchemaBuilder, SchemaRef};
use datatypes::vectors::{Float64Vector, StringVector, TimestampMillisecondVector};
use futures::Stream;
use store_api::storage::{RegionNumber, ScanRequest};

use super::scan::StreamScanAdapter;
use crate::error::Result;
use crate::metadata::{TableId, TableInfoBuilder, TableInfoRef, TableMetaBuilder, TableType};
use crate::Table;

const SERIE_NAME_FOO: &str = "foo";
const SERIE_NAME_BAR: &str = "bar";

const DAY_IN_MILLIS: i64 = 24 * 60 * 60 * 1000;
// Generate points for every 2 mins
const DEFAULT_POINT_INTERVAL_MILLIS: i64 = 2 * 60 * 1000;
const NROWS_PER_SERIE: usize = (DAY_IN_MILLIS / DEFAULT_POINT_INTERVAL_MILLIS) as usize;
const SERIE_FOO_POLY_PARAMS: [f64; 16] = [
    1.26277085e+06,
    -4.84295552e+07,
    3.64104698e+08,
    -1.32948521e+09,
    2.92478166e+09,
    -4.24460022e+09,
    4.24322625e+09,
    -2.97541474e+09,
    1.46478283e+09,
    -4.99073014e+08,
    1.14090304e+08,
    -1.65938476e+07,
    1.40601459e+06,
    -5.87104861e+04,
    7.56869166e+02,
    8.72776522e+00,
];
const JITTER_VALUES: [f64; 7] = [0.93, 0.98, 1.0, 1.01, 0.95, 1.03, 1.10];

/// A table that generates data on query
///
/// This table contains three time series for demo purpose
#[derive(Debug, Clone)]
pub struct GenerativeTable {
    name: String,
    table_id: TableId,
    schema: SchemaRef,
    series: Arc<Vec<GenerativeSerie>>,
}

#[derive(Debug, Clone)]
struct GenerativeSerie {
    name: String,
}

impl GenerativeTable {
    pub fn new() -> GenerativeTable {
        let column_schemas = vec![
            ColumnSchema::new("generator", ConcreteDataType::string_datatype(), false),
            ColumnSchema::new("value", ConcreteDataType::float64_datatype(), false),
            ColumnSchema::new(
                "ts",
                ConcreteDataType::timestamp_datatype(TimeUnit::Millisecond),
                false,
            )
            .with_time_index(true),
        ];
        let schema = Arc::new(
            SchemaBuilder::try_from_columns(column_schemas)
                .unwrap()
                .build()
                .unwrap(),
        );

        GenerativeTable {
            name: "generators".to_owned(),
            table_id: 2,
            schema,
            series: Arc::new(vec![
                GenerativeSerie {
                    name: SERIE_NAME_FOO.to_owned(),
                },
                GenerativeSerie {
                    name: SERIE_NAME_BAR.to_owned(),
                },
            ]),
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    fn stream(&self, limit: usize) -> GenerativeTableStream {
        GenerativeTableStream {
            limit,
            schema: self.schema.clone(),
            series: self.series.clone(),
            finished: false,
        }
    }
}

impl Default for GenerativeTable {
    fn default() -> Self {
        Self::new()
    }
}

fn polyval_for_serie_foo(timestamp: f64) -> f64 {
    let x = timestamp / 1e8;
    let param_len = SERIE_FOO_POLY_PARAMS.len();

    let jitter = JITTER_VALUES[(timestamp / 1e5).powi(2) as usize % JITTER_VALUES.len()];

    let value = SERIE_FOO_POLY_PARAMS
        .iter()
        .enumerate()
        .map(|(d, p)| p * x.powi((param_len - 1 - d) as i32))
        .sum::<f64>()
        * jitter;

    value
}

struct GenerativeTableStream {
    limit: usize,
    schema: SchemaRef,
    series: Arc<Vec<GenerativeSerie>>,
    finished: bool,
}

impl GenerativeSerie {
    pub(crate) fn generate_data_for_timestamps(&self, timestamps: &[i64]) -> Vec<f64> {
        match self.name.as_ref() {
            SERIE_NAME_FOO => {
                // polyfitted curve
                timestamps
                    .iter()
                    .map(|ts| {
                        let normalized_ts = (ts % DAY_IN_MILLIS) as f64;
                        polyval_for_serie_foo(normalized_ts)
                    })
                    .collect()
            }
            SERIE_NAME_BAR => {
                // linear
                timestamps
                    .iter()
                    .map(|ts| (*ts as f64) * 4.20e-11)
                    .collect()
            }
            _ => unreachable!(),
        }
    }

    pub(crate) fn generate_name_column(&self, nrows: usize) -> Vec<String> {
        (0..nrows).map(|_| self.name.clone()).collect()
    }
}

/// This function generates `nrows` of timestamp that aligned by `DEFAULT_POINT_INTERVAL_MILLIS`.
fn generate_normalized_timestamp(nrows: usize) -> Vec<i64> {
    let now_as_millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    let latest_point = now_as_millis - (now_as_millis % DEFAULT_POINT_INTERVAL_MILLIS);

    (0..nrows)
        .map(|idx| latest_point - idx as i64 * DEFAULT_POINT_INTERVAL_MILLIS)
        .collect()
}

impl RecordBatchStream for GenerativeTableStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Stream for GenerativeTableStream {
    type Item = RecordBatchResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.finished {
            return Poll::Ready(None);
        }

        let rows_per_serie = self.limit / self.series.len();
        let time_series = generate_normalized_timestamp(rows_per_serie);

        let name_column = self
            .series
            .iter()
            .flat_map(|s| s.generate_name_column(rows_per_serie))
            .collect::<Vec<String>>();
        let value_column = self
            .series
            .iter()
            .flat_map(|s| s.generate_data_for_timestamps(time_series.as_ref()))
            .collect::<Vec<f64>>();
        let ts_column = self
            .series
            .iter()
            .flat_map(|_| time_series.clone())
            .collect::<Vec<i64>>();

        let batch = RecordBatch::new(
            self.schema.clone(),
            vec![
                Arc::new(StringVector::from(name_column)) as Arc<_>,
                Arc::new(Float64Vector::from_slice(value_column)) as Arc<_>,
                Arc::new(TimestampMillisecondVector::from_slice(ts_column)) as Arc<_>,
            ],
        )
        .unwrap();
        self.finished = true;

        Poll::Ready(Some(Ok(batch)))
    }
}

#[async_trait::async_trait]
impl Table for GenerativeTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_info(&self) -> TableInfoRef {
        Arc::new(
            TableInfoBuilder::default()
                .table_id(self.table_id)
                .name(&self.name)
                .catalog_name("greptime")
                .schema_name("public")
                .table_version(0)
                .table_type(TableType::Base)
                .meta(
                    TableMetaBuilder::default()
                        .schema(self.schema.clone())
                        .region_numbers(vec![0])
                        .primary_key_indices(vec![0])
                        .next_column_id(3)
                        .engine("test_engine")
                        .build()
                        .unwrap(),
                )
                .build()
                .unwrap(),
        )
    }

    async fn scan(
        &self,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<PhysicalPlanRef> {
        Ok(Arc::new(StreamScanAdapter::new(Box::pin(self.stream(
            limit.unwrap_or(NROWS_PER_SERIE * self.series.len()),
        )))))
    }

    async fn scan_to_stream(&self, request: ScanRequest) -> Result<SendableRecordBatchStream> {
        Ok(Box::pin(self.stream(
            request.limit.unwrap_or(NROWS_PER_SERIE * self.series.len()),
        )))
    }

    async fn flush(&self, _region_number: Option<RegionNumber>, _wait: Option<bool>) -> Result<()> {
        Ok(())
    }
}
