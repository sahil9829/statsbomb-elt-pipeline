# --------------------------------------------------------------------------- #
# Glue Data Catalog: one database per layer, and the processed-zone tables
# generated from contracts/processed_tables.json (the same contract the
# PySpark test-suite enforces), so Athena/dbt can query Spark output at once.
# --------------------------------------------------------------------------- #
resource "aws_glue_catalog_database" "this" {
  for_each = local.glue_databases

  name        = "${local.glue_prefix}_${each.key}"
  description = each.value
}

resource "aws_glue_catalog_table" "processed" {
  for_each = local.processed_tables

  name          = each.key
  database_name = aws_glue_catalog_database.this["processed"].name
  description   = each.value.description
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    EXTERNAL              = "TRUE"
    classification        = "parquet"
    "parquet.compression" = "SNAPPY"
  }

  storage_descriptor {
    location      = "s3://${local.lake_bucket}/processed/statsbomb/${each.key}/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      name                  = "parquet"
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
      parameters            = {
        "serialization.format" = "1"
      }
    }

    dynamic "columns" {
      for_each = each.value.columns
      content {
        name = columns.value.name
        type = columns.value.type
      }
    }
  }
}
