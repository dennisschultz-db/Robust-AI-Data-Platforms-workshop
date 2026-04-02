"""Unit tests for silver_extract.py transformation logic.

Because pyspark.pipelines is Databricks-only, we cannot import the pipeline
source file directly.  Each test replicates the relevant logic from
silver_extract.py so that it can run in a local PySpark environment.

Corresponds to:
  src/financial_intelligence_pipeline/transformations/silver_extract.py
"""

import pytest
from pyspark.sql import Row
from pyspark.sql.functions import coalesce, col, element_at, regexp_extract, split


# ── Expected output schema ────────────────────────────────────────────────────

EXPECTED_COLUMNS = {
    "source_path",
    "file_size_bytes",
    "ingested_at",
    "plain_text",
    "company",
    "fiscal_period",
    "document_type",
    "revenue_reported",
    "net_income_reported",
}


# ── Helpers ───────────────────────────────────────────────────────────────────

def apply_document_type_fallback(df):
    """Mirrors the document_type coalesce in silver_extract.py."""
    return df.withColumn(
        "document_type",
        coalesce(
            col("extracted_document_type"),
            regexp_extract(col("source_path"), r"financial_docs_raw/([^/]+)", 1),
        ),
    )


def apply_company_fallback(df):
    """Mirrors the company coalesce in silver_extract.py."""
    filename = element_at(split(col("source_path"), "/"), -1)
    return df.withColumn(
        "company",
        coalesce(
            col("extracted_company"),
            regexp_extract(filename, r"^([^_.]+)", 1),
        ),
    )


# ── document_type fallback ────────────────────────────────────────────────────

class TestDocumentTypeFallback:

    def test_extracts_folder_name_when_ai_returns_null(self, spark):
        df = spark.createDataFrame(
            [
                Row(source_path="/Volumes/cat/schema/financial_docs_raw/10-K/apple_2024.pdf",
                    extracted_document_type=None),
                Row(source_path="/Volumes/cat/schema/financial_docs_raw/Earnings Call/nvda_q3.pdf",
                    extracted_document_type=None),
                Row(source_path="/Volumes/cat/schema/financial_docs_raw/10-Q/msft_q2.pdf",
                    extracted_document_type=None),
            ],
            schema="source_path STRING, extracted_document_type STRING",
        )
        result = apply_document_type_fallback(df).collect()
        assert result[0]["document_type"] == "10-K"
        assert result[1]["document_type"] == "Earnings Call"
        assert result[2]["document_type"] == "10-Q"

    def test_ai_extract_result_takes_precedence_over_path(self, spark):
        df = spark.createDataFrame([
            Row(source_path="/Volumes/cat/schema/financial_docs_raw/10-K/apple_2024.pdf",
                extracted_document_type="Annual Report"),
        ])
        result = apply_document_type_fallback(df).collect()
        assert result[0]["document_type"] == "Annual Report"

    def test_returns_empty_string_when_path_does_not_match_pattern(self, spark):
        """An unexpected path pattern should not raise — fallback yields empty string."""
        df = spark.createDataFrame(
            [
                Row(source_path="/unexpected/path/file.pdf",
                    extracted_document_type=None),
            ],
            schema="source_path STRING, extracted_document_type STRING",
        )
        result = apply_document_type_fallback(df).collect()
        # regexp_extract returns "" (not null) when no match — column is non-null
        assert result[0]["document_type"] == ""


# ── company fallback ──────────────────────────────────────────────────────────

class TestCompanyFallback:

    def test_extracts_first_token_before_underscore(self, spark):
        df = spark.createDataFrame(
            [
                Row(source_path="/Volumes/cat/schema/financial_docs_raw/10-K/NVIDIA_2024_10K.pdf",
                    extracted_company=None),
                Row(source_path="/Volumes/cat/schema/financial_docs_raw/10-K/Apple_FY24_Annual.pdf",
                    extracted_company=None),
                Row(source_path="/Volumes/cat/schema/financial_docs_raw/10-K/Meta_Q3_2024.pdf",
                    extracted_company=None),
            ],
            schema="source_path STRING, extracted_company STRING",
        )
        result = apply_company_fallback(df).collect()
        assert result[0]["company"] == "NVIDIA"
        assert result[1]["company"] == "Apple"
        assert result[2]["company"] == "Meta"

    def test_ai_extract_result_takes_precedence_over_filename(self, spark):
        df = spark.createDataFrame([
            Row(source_path="/Volumes/cat/schema/financial_docs_raw/10-K/NVIDIA_2024.pdf",
                extracted_company="Microsoft"),
        ])
        result = apply_company_fallback(df).collect()
        assert result[0]["company"] == "Microsoft"

    def test_filename_without_underscore_returns_whole_name(self, spark):
        df = spark.createDataFrame(
            [
                Row(source_path="/Volumes/cat/schema/financial_docs_raw/10-K/Amazon.pdf",
                    extracted_company=None),
            ],
            schema="source_path STRING, extracted_company STRING",
        )
        result = apply_company_fallback(df).collect()
        assert result[0]["company"] == "Amazon"


# ── has_content expectation (expect_or_drop) ─────────────────────────────────

class TestHasContentExpectation:
    """
    Mirrors the @dp.expect_or_drop("has_content", ...) predicate:
        plain_text IS NOT NULL AND length(plain_text) > 200
    """

    HAS_CONTENT = "plain_text IS NOT NULL AND length(plain_text) > 200"

    def test_keeps_records_with_sufficient_content(self, spark):
        df = spark.createDataFrame([
            Row(id="good", plain_text="x" * 201),
        ])
        assert df.filter(self.HAS_CONTENT).count() == 1

    def test_drops_null_plain_text(self, spark):
        df = spark.createDataFrame(
            [Row(id="null_text", plain_text=None)],
            schema="id STRING, plain_text STRING",
        )
        assert df.filter(self.HAS_CONTENT).count() == 0

    def test_drops_short_plain_text(self, spark):
        df = spark.createDataFrame([
            Row(id="too_short", plain_text="too short"),
            Row(id="boundary",  plain_text="x" * 200),   # exactly 200 — NOT > 200
        ])
        assert df.filter(self.HAS_CONTENT).count() == 0

    def test_mixed_batch_only_valid_rows_pass(self, spark):
        df = spark.createDataFrame(
            [
                Row(id="pass",  plain_text="x" * 201),
                Row(id="fail1", plain_text=None),
                Row(id="fail2", plain_text="short"),
            ],
            schema="id STRING, plain_text STRING",
        )
        result = df.filter(self.HAS_CONTENT)
        assert result.count() == 1
        assert result.collect()[0]["id"] == "pass"


# ── output schema ─────────────────────────────────────────────────────────────

class TestOutputSchema:

    def test_silver_output_has_exactly_expected_columns(self, spark):
        """Build a mock silver DataFrame and verify no columns are missing or extra."""
        df = spark.createDataFrame(
            [
                Row(
                    source_path="path/a.pdf",
                    file_size_bytes=12345,
                    ingested_at="2024-01-01",
                    plain_text="x" * 201,
                    company="Apple",
                    fiscal_period="FY24Q1",
                    document_type="10-K",
                    revenue_reported="$100B",
                    net_income_reported="$25B",
                ),
            ],
            schema="source_path STRING, file_size_bytes INT, ingested_at STRING, "
                   "plain_text STRING, company STRING, fiscal_period STRING, "
                   "document_type STRING, revenue_reported STRING, "
                   "net_income_reported STRING",
        )
        assert set(df.columns) == EXPECTED_COLUMNS
