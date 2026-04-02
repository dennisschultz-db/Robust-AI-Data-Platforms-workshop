# Databricks Lakeflow Spark Declarative Pipeline — Silver Layer
#
# Parses PDF binary content into plain text using ai_parse_document(), then uses
# ai_extract() to pull structured fields (company, period, document type,
# revenue, net income) from the plain text.
#
# ai_parse_document() returns a VARIANT.  We extract plain text directly from
# the VARIANT using :document:elements path syntax — no to_json() round-trip
# needed.  Plain text is cleaner input for ai_extract(), ai_classify(), and
# the Vector Search embedding model than a JSON-wrapped string.
#
# Data quality expectations:
#   - DROP rows where plain text is null or suspiciously short (< 200 chars)
#   - WARN (but keep) rows where company name could not be extracted

from pyspark import pipelines as dp
from pyspark.sql.functions import col, coalesce, element_at, expr, regexp_extract, split


@dp.table(
    name="financial_docs_silver",
    comment=(
        "Plain text and AI-extracted structured fields from financial PDFs. "
        "Produced by ai_parse_document() and ai_extract()."
    ),
    table_properties={
        "quality":                          "silver",
        "delta.autoOptimize.optimizeWrite": "true",
    },
)

# Records without content or with very short content will be dropped
@dp.expect_or_drop("has_content",   "plain_text IS NOT NULL AND length(plain_text) > 200")
# Records without metadata will be accepted, but flagged
@dp.expect("has_company",        "company IS NOT NULL")
@dp.expect("has_doc_type",       "document_type IS NOT NULL")
@dp.expect("has_fiscal_period",  "fiscal_period IS NOT NULL")
def financial_docs_silver():
    # Step 1: Read the bronze streaming table.
    df = spark.readStream.table("financial_docs_bronze")

    # Step 2: Parse the raw PDF bytes into plain text.
    # ai_parse_document() returns a VARIANT with a document:elements array.
    # We concatenate the content field of each element with double newlines to
    # produce clean prose — better for ai_extract(), ai_classify(), and embeddings
    # than a JSON-serialised string.
    df = df.withColumn(
        "plain_text",
        expr("""
            concat_ws('\n\n',
                transform(
                    try_cast(ai_parse_document(content):document:elements AS ARRAY<VARIANT>),
                    el -> try_cast(el:content AS STRING)
                )
            )
        """),
    )

    # Step 3: Extract key structured fields from the plain text.
    # ai_extract() takes ARRAY<STRING> labels and returns a STRUCT whose
    # field names match those labels exactly.
    df = df.withColumn(
        "extracted",
        expr(
            "ai_extract(plain_text, array("
            "  'company',"
            "  'fiscal_period',"
            "  'document_type',"
            "  'revenue',"
            "  'net_income'"
            "))"
        ),
    )

    # Step 3a: Fallback for nulls in document_type and company.
    #   - document_type: use the path segment immediately after 'financial_docs_raw'
    #   - company: use the first underscore-delimited token of the filename
    filename = element_at(split(col("source_path"), "/"), -1)

    df = df.withColumn(
        "document_type",
        coalesce(
            col("extracted.document_type"),
            regexp_extract(col("source_path"), r"financial_docs_raw/([^/]+)", 1),
        ),
    ).withColumn(
        "company",
        coalesce(
            col("extracted.company"),
            regexp_extract(filename, r"^([^_.]+)", 1),
        ),
    )

    # Step 4: Select and rename the final columns.
    df = df.select(
        "source_path",
        "file_size_bytes",
        "ingested_at",
        "plain_text",
        "company",
        col("extracted.fiscal_period").alias("fiscal_period"),
        "document_type",
        col("extracted.revenue").alias("revenue_reported"),
        col("extracted.net_income").alias("net_income_reported"),
    )

    return df
