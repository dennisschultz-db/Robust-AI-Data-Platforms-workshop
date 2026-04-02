# Databricks Lakeflow Spark Declarative Pipeline — Bronze Layer
#
# Ingests raw PDF binary files from the shared Volume using Auto Loader (cloudFiles).
# Each file is stored as a single row: the binary content plus file metadata.
# No transformation is applied at this layer — raw fidelity is preserved.

from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp

catalog       = spark.conf.get("pipeline.catalog",       "platform-workshop")
shared_schema = spark.conf.get("pipeline.shared_schema", "00_shared")

SOURCE_PATH = f"/Volumes/{catalog}/{shared_schema}/financial_docs_raw/"


@dp.table(
    name="financial_docs_bronze",
    comment=(
        "Raw binary content of financial PDFs ingested from the shared Volume. "
        "One row per file. No transformation applied."
    ),
    table_properties={
        "quality":                     "bronze",
        "delta.autoOptimize.optimizeWrite": "true",
    },
)
def financial_docs_bronze():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format",        "binaryFile")
        .option("pathGlobFilter",            "*.pdf")
        .option("recursiveFileLookup",       "true")
        .option("cloudFiles.inferColumnTypes", "false")
        .load(SOURCE_PATH)
        .select(
            col("_metadata.file_path").alias("source_path"),
            col("content"),
            col("length").alias("file_size_bytes"),
            current_timestamp().alias("ingested_at"),
        )
    )
