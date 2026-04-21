# Databricks Lakeflow Spark Declarative Pipeline — Silver Layer
#
# Parses PDF binary content using ai_parse_document(), then uses ai_extract()
# to pull structured fields (company, period, document type, revenue, net income)
# from the plain text.
#
# ai_parse_document() returns a VARIANT.  We persist the full VARIANT as
# `parsed_document` so that downstream jobs can call ai_prep_search() for
# semantic chunking.  Plain text is derived from the VARIANT for ai_extract()
# and ai_classify().
#
# Data quality expectations:
#   - DROP rows where plain text is null or suspiciously short (< 200 chars)
#   - WARN (but keep) rows where company name could not be extracted

from pyspark import pipelines as dp
from pyspark.sql.functions import col, coalesce, element_at, expr, regexp_extract, split


@dp.table(
    name="financial_docs_silver",
    comment=(
        "Parsed documents and AI-extracted structured fields from financial PDFs. "
        "Produced by ai_parse_document() and ai_extract(). The parsed_document "
        "VARIANT is preserved for downstream ai_prep_search() chunking."
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

    # Step 2: Parse the raw PDF bytes.
    # ai_parse_document() returns a VARIANT.  We persist the full VARIANT as
    # `parsed_document` so that the build_search_table job can call
    # ai_prep_search() for semantic chunking without re-parsing.
    df = df.withColumn(
        "parsed_document",
        expr("ai_parse_document(content)"),
    )

    # Derive plain_text from the VARIANT — still needed for ai_extract() below.
    # Concatenate the content field of each element with double newlines to
    # produce clean prose for downstream AI functions.
    df = df.withColumn(
        "plain_text",
        expr("""
            concat_ws('\n\n',
                transform(
                    try_cast(
                        parsed_document:document:elements AS ARRAY<VARIANT>),
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
        expr("""
            ai_extract(
                plain_text, 
                array(
                    'company',
                    'fiscal_period',
                    'document_type',
                    'revenue',
                    'net_income'
                )
            )
        """),
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

    # Step 3b: Normalize company names to canonical forms.
    # ai_extract() may return variations like "amazon", "Amazon.com",
    # "NVIDIA", "Nvidia Corp", etc.  ai_classify() maps each value to the
    # closest canonical name from the provided array.
    df = df.withColumn(
        "company",
        expr("""
            ai_classify(
                company, 
                array(
                    'Amazon.com, Inc.',
                    'Apple Inc.',
                    'Meta Platforms, Inc.',
                    'Microsoft Corporation',
                    'NVIDIA Corporation'
                )
            )
        """),
    )

    # Step 4: Select and rename the final columns.
    df = df.select(
        "source_path",
        "file_size_bytes",
        "ingested_at",
        "parsed_document",
        "plain_text",
        "company",
        col("extracted.fiscal_period").alias("fiscal_period"),
        "document_type",
        col("extracted.revenue").alias("revenue_reported"),
        col("extracted.net_income").alias("net_income_reported"),
    )

    return df
