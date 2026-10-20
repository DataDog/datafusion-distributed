#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::sync::Arc;

    use datafusion::common::stats::Precision;
    use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
    use datafusion::datasource::source::DataSourceExec;
    use datafusion::physical_plan::statistics::{StatisticsArgs, StatisticsContext};
    use datafusion_distributed_iceberg::IcebergExt;
    use datafusion_distributed_iceberg_test_tools::{FixtureCatalog, IcebergTestHarness};
    use iceberg::TableIdent;
    use iceberg::spec::{
        DataContentType, DataFileBuilder, DataFileFormat, Datum, ListType, NestedField,
        PrimitiveType, Schema, Struct, StructType, Type,
    };

    #[tokio::test]
    async fn selected_nested_columns_sum_leaves_and_keep_missing_leaves_unknown()
    -> Result<(), Box<dyn Error>> {
        let harness = nested_fixture().await?;
        for (id, expected) in [(1, Precision::Inexact(300)), (2, Precision::Absent)] {
            let plan = harness
                .physical_plan(&format!("SELECT details FROM nested WHERE id = {id}"))
                .await?;
            let mut checked = false;
            plan.apply(|node| {
                if node.is::<DataSourceExec>() {
                    let stats =
                        StatisticsContext::new().compute(node.as_ref(), &StatisticsArgs::new())?;
                    let column = node.schema().index_of("details")?;
                    assert_eq!(stats.column_statistics[column].byte_size, expected);
                    assert_eq!(
                        stats.column_statistics[column].null_count,
                        Precision::Absent
                    );
                    assert_eq!(stats.column_statistics[column].min_value, Precision::Absent);
                    assert_eq!(stats.column_statistics[column].max_value, Precision::Absent);
                    assert_eq!(stats.num_rows, Precision::Inexact(10));
                    checked = true;
                }
                Ok(TreeNodeRecursion::Continue)
            })?;
            assert!(checked);
        }
        Ok(())
    }

    // A table of two synthetic whole files that are never opened. The first sizes every
    // leaf of `details`; the second has no size for the list element, so the struct's
    // size cannot be reconstructed from its leaves.
    async fn nested_fixture() -> Result<IcebergTestHarness, Box<dyn Error>> {
        let schema = Schema::builder()
            .with_fields([
                Arc::new(NestedField::required(
                    1,
                    "id",
                    Type::Primitive(PrimitiveType::Int),
                )),
                Arc::new(NestedField::optional(
                    2,
                    "details",
                    Type::Struct(StructType::new(vec![
                        Arc::new(NestedField::optional(
                            3,
                            "name",
                            Type::Primitive(PrimitiveType::String),
                        )),
                        Arc::new(NestedField::optional(
                            4,
                            "values",
                            Type::List(ListType::new(Arc::new(NestedField::optional(
                                5,
                                "element",
                                Type::Primitive(PrimitiveType::Long),
                            )))),
                        )),
                    ])),
                )),
            ])
            .build()?;
        let fixtures = FixtureCatalog::try_new().await?;
        let table = fixtures
            .table(TableIdent::from_strs(["default", "nested"])?, schema)
            .create()
            .await?;
        // Field ids as the catalog assigned them when creating the table.
        let schema = Arc::clone(table.table().metadata().current_schema());
        let field_id = |name: &str| {
            schema
                .field_id_by_name(name)
                .ok_or_else(|| format!("no field {name}"))
        };
        let (id, name, element) = (
            field_id("id")?,
            field_id("details.name")?,
            field_id("details.values.element")?,
        );
        let location = table.table().metadata().location().to_owned();
        let mut files = Vec::new();
        for file in [1, 2] {
            let sizes = if file == 1 {
                [(id, 40), (name, 100), (element, 200)]
                    .into_iter()
                    .collect()
            } else {
                [(id, 40), (name, 900)].into_iter().collect()
            };
            files.push(
                DataFileBuilder::default()
                    .content(DataContentType::Data)
                    .file_format(DataFileFormat::Parquet)
                    .file_path(format!("{location}/data/unopened-nested-{file}.parquet"))
                    .partition(Struct::empty())
                    .record_count(10)
                    .file_size_in_bytes(1000)
                    .column_sizes(sizes)
                    .lower_bounds([(id, Datum::int(file))].into())
                    .upper_bounds([(id, Datum::int(file))].into())
                    .build()?,
            );
        }
        Ok(IcebergTestHarness::builder()
            .with_session_extension(|state| state.with_iceberg_integration(Default::default()))
            .with_table(table)
            .with_data_files(files)
            .with_config_option("iceberg.eager_file_planning", true)
            .with_config_option("iceberg.column_stats_enabled", true)
            .build()
            .await?)
    }
}
