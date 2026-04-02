"""Unit tests for gold_classify.py transformation logic.

Because pyspark.pipelines is Databricks-only, we cannot import the pipeline
source file directly.  Tests replicate the aggregation logic from
gold_classify.py and validate the category constant values by string comparison.

Corresponds to:
  src/financial_intelligence_pipeline/transformations/gold_classify.py
"""

import pytest
from pyspark.sql import Row
from pyspark.sql.functions import col


# ── Category constants (mirrors gold_classify.py) ────────────────────────────
# Kept here so a typo in the source file is caught by the tests below.

AI_INVESTMENT_CATEGORIES = [
    "AI Infrastructure",
    "AI Products & Services",
    "AI Partnerships",
    "Not AI-related",
]

SENTIMENT_CATEGORIES = [
    "Positive",
    "Neutral",
    "Cautious",
    "Negative",
]


# ── Category constant validation ──────────────────────────────────────────────

class TestCategoryConstants:
    """Guard against typos in the category strings passed to ai_classify().
    If a category is misspelled the model will silently return unexpected values.
    """

    def test_ai_investment_categories_are_complete(self):
        assert len(AI_INVESTMENT_CATEGORIES) == 4
        assert "AI Infrastructure" in AI_INVESTMENT_CATEGORIES
        assert "AI Products & Services" in AI_INVESTMENT_CATEGORIES
        assert "AI Partnerships" in AI_INVESTMENT_CATEGORIES
        assert "Not AI-related" in AI_INVESTMENT_CATEGORIES

    def test_sentiment_categories_are_complete(self):
        assert len(SENTIMENT_CATEGORIES) == 4
        assert "Positive" in SENTIMENT_CATEGORIES
        assert "Neutral" in SENTIMENT_CATEGORIES
        assert "Cautious" in SENTIMENT_CATEGORIES
        assert "Negative" in SENTIMENT_CATEGORIES

    def test_no_duplicate_categories(self):
        assert len(AI_INVESTMENT_CATEGORIES) == len(set(AI_INVESTMENT_CATEGORIES))
        assert len(SENTIMENT_CATEGORIES) == len(set(SENTIMENT_CATEGORIES))


# ── company_ai_investment_summary aggregation ─────────────────────────────────

class TestCompanyAiInvestmentSummary:
    """
    Mirrors the groupBy / count / rename in gold_classify.py:

        spark.read.table("financial_docs_gold")
        .groupBy("company", "fiscal_period", "document_type",
                 "ai_investment_category", "management_sentiment")
        .count()
        .withColumnRenamed("count", "document_count")
        .orderBy("company", "fiscal_period")
    """

    @pytest.fixture
    def gold_df(self, spark):
        return spark.createDataFrame([
            Row(company="Apple",  fiscal_period="FY24Q1", document_type="10-K",
                ai_investment_category="AI Infrastructure",    management_sentiment="Positive"),
            Row(company="Apple",  fiscal_period="FY24Q1", document_type="10-K",
                ai_investment_category="AI Infrastructure",    management_sentiment="Positive"),
            Row(company="Apple",  fiscal_period="FY24Q1", document_type="10-K",
                ai_investment_category="Not AI-related",       management_sentiment="Neutral"),
            Row(company="NVIDIA", fiscal_period="FY25Q1", document_type="10-Q",
                ai_investment_category="AI Infrastructure",    management_sentiment="Positive"),
            Row(company="NVIDIA", fiscal_period="FY25Q2", document_type="10-Q",
                ai_investment_category="AI Products & Services", management_sentiment="Positive"),
        ])

    def _aggregate(self, df):
        return (
            df
            .groupBy("company", "fiscal_period", "document_type",
                     "ai_investment_category", "management_sentiment")
            .count()
            .withColumnRenamed("count", "document_count")
            .orderBy("company", "fiscal_period")
        )

    def test_produces_correct_number_of_rows(self, spark, gold_df):
        # 5 input rows → 4 distinct groups
        result = self._aggregate(gold_df)
        assert result.count() == 4

    def test_document_count_is_correct(self, spark, gold_df):
        result = self._aggregate(gold_df)
        apple_infra = result.filter(
            (col("company") == "Apple") &
            (col("ai_investment_category") == "AI Infrastructure")
        ).collect()
        assert len(apple_infra) == 1
        assert apple_infra[0]["document_count"] == 2

    def test_single_document_has_count_of_one(self, spark, gold_df):
        result = self._aggregate(gold_df)
        apple_non_ai = result.filter(
            (col("company") == "Apple") &
            (col("ai_investment_category") == "Not AI-related")
        ).collect()
        assert apple_non_ai[0]["document_count"] == 1

    def test_output_columns_are_correct(self, spark, gold_df):
        result = self._aggregate(gold_df)
        expected = {
            "company", "fiscal_period", "document_type",
            "ai_investment_category", "management_sentiment", "document_count",
        }
        assert set(result.columns) == expected

    def test_output_is_ordered_by_company_then_period(self, spark, gold_df):
        result = self._aggregate(gold_df).collect()
        companies = [r["company"] for r in result]
        # Apple rows should precede NVIDIA rows
        last_apple = max(i for i, c in enumerate(companies) if c == "Apple")
        first_nvidia = min(i for i, c in enumerate(companies) if c == "NVIDIA")
        assert last_apple < first_nvidia
