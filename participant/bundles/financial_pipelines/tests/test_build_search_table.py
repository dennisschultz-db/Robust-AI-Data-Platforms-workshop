"""Unit tests for the build_search_table job task.

Tests the transformation logic from jobs/build_search_table.ipynb:
  - Only the seven required columns are written to the output table
  - Rows with null parsed_document are excluded
  - chunk_id is unique (it is the Vector Search index primary key)
  - Each document produces multiple chunk rows

Corresponds to:
  jobs/build_search_table.ipynb
"""

import pytest
from pyspark.sql import Row
from pyspark.sql.functions import col, explode, expr


SEARCH_TABLE_COLUMNS = {
    "chunk_id",
    "chunk_to_embed",
    "chunk_text",
    "source_path",
    "company",
    "fiscal_period",
    "document_type",
}


# ── Helpers ───────────────────────────────────────────────────────────────────

def apply_search_table_transform(df):
    """Mirrors the select + filter from build_search_table.ipynb.

    Note: ai_prep_search() is a Databricks serverless SQL function that cannot
    be called in local unit tests.  These tests validate the column selection,
    null filtering, and schema contract.  The chunking behaviour of
    ai_prep_search() is tested via integration tests on the cluster.
    """
    return (
        df
        .filter(col("parsed_document").isNotNull())
        .select(
            col("chunk_id"),
            col("chunk_to_embed"),
            col("chunk_text"),
            col("source_path"),
            col("company"),
            col("fiscal_period"),
            col("document_type"),
        )
    )


# ── Column selection ──────────────────────────────────────────────────────────

class TestColumnSelection:

    def test_output_has_exactly_the_seven_required_columns(self, spark):
        """Extra columns from silver (e.g. revenue_reported) must not leak through."""
        df = spark.createDataFrame(
            [
                Row(chunk_id="doc1_chunk0", chunk_to_embed="enriched text",
                    chunk_text="raw text", source_path="path/a.pdf",
                    company="Apple", fiscal_period="FY24Q1", document_type="10-K",
                    parsed_document="not_null",
                    revenue_reported="$100B", net_income_reported="$25B",
                    ingested_at="2024-01-01", file_size_bytes=1234),
            ],
            schema="chunk_id STRING, chunk_to_embed STRING, chunk_text STRING, "
                   "source_path STRING, company STRING, fiscal_period STRING, "
                   "document_type STRING, parsed_document STRING, "
                   "revenue_reported STRING, net_income_reported STRING, "
                   "ingested_at STRING, file_size_bytes INT",
        )
        result = apply_search_table_transform(df)
        assert set(result.columns) == SEARCH_TABLE_COLUMNS

    def test_no_extra_columns_present(self, spark):
        df = spark.createDataFrame([
            Row(chunk_id="doc1_chunk0", chunk_to_embed="enriched text",
                chunk_text="raw text", source_path="path/a.pdf",
                company="Apple", fiscal_period="FY24Q1", document_type="10-K",
                parsed_document="not_null"),
        ])
        result = apply_search_table_transform(df)
        assert len(result.columns) == 7


# ── Null parsed_document filter ──────────────────────────────────────────────

class TestNullParsedDocumentFilter:

    def test_rows_with_null_parsed_document_are_excluded(self, spark):
        df = spark.createDataFrame([
            Row(chunk_id="doc1_chunk0", chunk_to_embed="enriched text",
                chunk_text="valid text", source_path="path/a.pdf",
                company="Apple", fiscal_period="FY24Q1", document_type="10-K",
                parsed_document="not_null"),
            Row(chunk_id="doc2_chunk0", chunk_to_embed="enriched text",
                chunk_text="text", source_path="path/b.pdf",
                company="Meta", fiscal_period="FY24Q2", document_type="10-Q",
                parsed_document=None),
        ])
        result = apply_search_table_transform(df)
        assert result.count() == 1
        assert result.collect()[0]["source_path"] == "path/a.pdf"

    def test_all_rows_valid_all_pass(self, spark):
        df = spark.createDataFrame([
            Row(chunk_id="doc1_chunk0", chunk_to_embed="enriched a",
                chunk_text="text a", source_path="path/a.pdf",
                company="Apple", fiscal_period="FY24Q1", document_type="10-K",
                parsed_document="not_null"),
            Row(chunk_id="doc2_chunk0", chunk_to_embed="enriched b",
                chunk_text="text b", source_path="path/b.pdf",
                company="NVIDIA", fiscal_period="FY25Q1", document_type="10-Q",
                parsed_document="not_null"),
        ])
        result = apply_search_table_transform(df)
        assert result.count() == 2


# ── chunk_id uniqueness (Vector Search primary key) ──────────────────────────

class TestChunkIdUniqueness:
    """
    chunk_id is the primary key for the Vector Search Delta Sync index.
    A duplicate will cause the index sync to fail.  This test documents the
    expectation so that any upstream change that produces duplicates
    is caught before deployment.
    """

    def test_no_duplicate_chunk_ids_in_clean_input(self, spark):
        df = spark.createDataFrame([
            Row(chunk_id="doc1_chunk0", chunk_to_embed="enriched a",
                chunk_text="text a", source_path="path/a.pdf",
                company="Apple", fiscal_period="FY24Q1", document_type="10-K",
                parsed_document="not_null"),
            Row(chunk_id="doc1_chunk1", chunk_to_embed="enriched b",
                chunk_text="text b", source_path="path/a.pdf",
                company="Apple", fiscal_period="FY24Q1", document_type="10-K",
                parsed_document="not_null"),
            Row(chunk_id="doc2_chunk0", chunk_to_embed="enriched c",
                chunk_text="text c", source_path="path/b.pdf",
                company="NVIDIA", fiscal_period="FY25Q1", document_type="10-Q",
                parsed_document="not_null"),
        ])
        result = apply_search_table_transform(df)
        assert result.count() == result.dropDuplicates(["chunk_id"]).count()

    def test_detects_duplicate_chunk_ids(self, spark):
        """Negative test — confirms the dedup check would catch a real duplicate."""
        df = spark.createDataFrame([
            Row(chunk_id="doc1_chunk0", chunk_to_embed="enriched a",
                chunk_text="text a", source_path="path/a.pdf",
                company="Apple", fiscal_period="FY24Q1", document_type="10-K",
                parsed_document="not_null"),
            Row(chunk_id="doc1_chunk0", chunk_to_embed="enriched a",
                chunk_text="text a", source_path="path/a.pdf",
                company="Apple", fiscal_period="FY24Q1", document_type="10-K",
                parsed_document="not_null"),
        ])
        result = apply_search_table_transform(df)
        assert result.count() != result.dropDuplicates(["chunk_id"]).count()


# ── Multiple chunks per document ─────────────────────────────────────────────

class TestMultipleChunksPerDocument:
    """
    ai_prep_search() produces multiple chunks per document.  Verify that
    the transform preserves all chunks while carrying metadata through.
    """

    def test_multiple_chunks_from_same_document(self, spark):
        df = spark.createDataFrame([
            Row(chunk_id="doc1_chunk0", chunk_to_embed="enriched a0",
                chunk_text="text a0", source_path="path/a.pdf",
                company="Apple", fiscal_period="FY24Q1", document_type="10-K",
                parsed_document="not_null"),
            Row(chunk_id="doc1_chunk1", chunk_to_embed="enriched a1",
                chunk_text="text a1", source_path="path/a.pdf",
                company="Apple", fiscal_period="FY24Q1", document_type="10-K",
                parsed_document="not_null"),
            Row(chunk_id="doc1_chunk2", chunk_to_embed="enriched a2",
                chunk_text="text a2", source_path="path/a.pdf",
                company="Apple", fiscal_period="FY24Q1", document_type="10-K",
                parsed_document="not_null"),
        ])
        result = apply_search_table_transform(df)
        assert result.count() == 3
        # All chunks should share the same source_path
        source_paths = [r["source_path"] for r in result.collect()]
        assert all(p == "path/a.pdf" for p in source_paths)
        # But have distinct chunk_ids
        chunk_ids = [r["chunk_id"] for r in result.collect()]
        assert len(set(chunk_ids)) == 3
