"""Unit tests for the build_search_table job task.

Tests the transformation logic from jobs/build_search_table.ipynb:
  - Only the five required columns are written to the output table
  - Rows with null plain_text are excluded
  - source_path is unique (it is the Vector Search index primary key)

Corresponds to:
  jobs/build_search_table.ipynb
"""

import pytest
from pyspark.sql import Row
from pyspark.sql.functions import col


SEARCH_TABLE_COLUMNS = {
    "source_path",
    "plain_text",
    "company",
    "fiscal_period",
    "document_type",
}


# ── Helpers ───────────────────────────────────────────────────────────────────

def apply_search_table_transform(df):
    """Mirrors the select + filter from build_search_table.ipynb."""
    return (
        df
        .select(
            col("source_path"),
            col("plain_text"),
            col("company"),
            col("fiscal_period"),
            col("document_type"),
        )
        .filter(col("plain_text").isNotNull())
    )


# ── Column selection ──────────────────────────────────────────────────────────

class TestColumnSelection:

    def test_output_has_exactly_the_five_required_columns(self, spark):
        """Extra columns from silver (e.g. revenue_reported) must not leak through."""
        df = spark.createDataFrame(
            [
                Row(source_path="path/a.pdf", plain_text="text", company="Apple",
                    fiscal_period="FY24Q1", document_type="10-K",
                    revenue_reported="$100B", net_income_reported="$25B",
                    ingested_at="2024-01-01", file_size_bytes=1234),
            ],
            schema="source_path STRING, plain_text STRING, company STRING, "
                   "fiscal_period STRING, document_type STRING, "
                   "revenue_reported STRING, net_income_reported STRING, "
                   "ingested_at STRING, file_size_bytes INT",
        )
        result = apply_search_table_transform(df)
        assert set(result.columns) == SEARCH_TABLE_COLUMNS

    def test_no_extra_columns_present(self, spark):
        df = spark.createDataFrame([
            Row(source_path="path/a.pdf", plain_text="text", company="Apple",
                fiscal_period="FY24Q1", document_type="10-K"),
        ])
        result = apply_search_table_transform(df)
        assert len(result.columns) == 5


# ── Null plain_text filter ───────────────────────────────────────────────────

class TestNullParsedTextFilter:

    def test_rows_with_null_plain_text_are_excluded(self, spark):
        df = spark.createDataFrame([
            Row(source_path="path/a.pdf", plain_text="valid text",
                company="Apple", fiscal_period="FY24Q1", document_type="10-K"),
            Row(source_path="path/b.pdf", plain_text=None,
                company="Meta", fiscal_period="FY24Q2", document_type="10-Q"),
        ])
        result = apply_search_table_transform(df)
        assert result.count() == 1
        assert result.collect()[0]["source_path"] == "path/a.pdf"

    def test_empty_string_is_not_filtered_out(self, spark):
        """Only NULL is filtered; empty string passes through (may later be a quality issue)."""
        df = spark.createDataFrame([
            Row(source_path="path/a.pdf", plain_text="",
                company="Apple", fiscal_period="FY24Q1", document_type="10-K"),
        ])
        result = apply_search_table_transform(df)
        assert result.count() == 1

    def test_all_rows_valid_all_pass(self, spark):
        df = spark.createDataFrame([
            Row(source_path="path/a.pdf", plain_text="text a",
                company="Apple",  fiscal_period="FY24Q1", document_type="10-K"),
            Row(source_path="path/b.pdf", plain_text="text b",
                company="NVIDIA", fiscal_period="FY25Q1", document_type="10-Q"),
        ])
        result = apply_search_table_transform(df)
        assert result.count() == 2


# ── source_path uniqueness (Vector Search primary key) ───────────────────────

class TestSourcePathUniqueness:
    """
    source_path is the primary key for the Vector Search Delta Sync index.
    A duplicate will cause the index sync to fail.  This test documents the
    expectation so that any upstream silver change that produces duplicates
    is caught before deployment.
    """

    def test_no_duplicate_source_paths_in_clean_input(self, spark):
        df = spark.createDataFrame([
            Row(source_path="path/a.pdf", plain_text="text a",
                company="Apple",  fiscal_period="FY24Q1", document_type="10-K"),
            Row(source_path="path/b.pdf", plain_text="text b",
                company="NVIDIA", fiscal_period="FY25Q1", document_type="10-Q"),
        ])
        result = apply_search_table_transform(df)
        assert result.count() == result.dropDuplicates(["source_path"]).count()

    def test_detects_duplicate_source_paths(self, spark):
        """Negative test — confirms the dedup check would catch a real duplicate."""
        df = spark.createDataFrame([
            Row(source_path="path/a.pdf", plain_text="text a",
                company="Apple", fiscal_period="FY24Q1", document_type="10-K"),
            Row(source_path="path/a.pdf", plain_text="text a",
                company="Apple", fiscal_period="FY24Q1", document_type="10-K"),
        ])
        result = apply_search_table_transform(df)
        assert result.count() != result.dropDuplicates(["source_path"]).count()
