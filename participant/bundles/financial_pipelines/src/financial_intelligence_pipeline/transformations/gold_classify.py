# Databricks Lakeflow Spark Declarative Pipeline — Gold Layer
#
# Applies ai_classify() to add two analytical dimensions to every document:
#   1. ai_investment_category — how AI-related is this document's content?
#   2. management_sentiment   — what tone does management take overall?
#
# Also produces a summary aggregate table (company_ai_investment_summary)
# that rolls up document counts by company, period, and AI category —
# the key analytical output participants will query in the workshop exercises.

from pyspark import pipelines as dp
from pyspark.sql.functions import col, expr

AI_INVESTMENT_CATEGORIES = (
    "array('AI Infrastructure', 'AI Products & Services', 'AI Partnerships', 'Not AI-related')"
)
SENTIMENT_CATEGORIES = "array('Positive', 'Neutral', 'Cautious', 'Negative')"


@dp.materialized_view(
    name="financial_docs_gold",
    comment=(
        "Classified financial documents with AI investment category and management sentiment. "
        "One row per document."
    ),
    table_properties={
        "quality":                          "gold",
        "delta.autoOptimize.optimizeWrite": "true",
    },
)
def financial_docs_gold():
    return (
        spark.read.table("financial_docs_silver")
        .withColumn(
            "ai_investment_category",
            expr(f"ai_classify(plain_text, {AI_INVESTMENT_CATEGORIES})"),
        )
        .withColumn(
            "management_sentiment",
            expr(f"ai_classify(plain_text, {SENTIMENT_CATEGORIES})"),
        )
        # Drop the plain_text column to keep the gold table lean
        .drop("plain_text")
    )


@dp.materialized_view(
    name="company_ai_investment_summary",
    comment=(
        "Aggregated AI investment signals by company, fiscal period, and category. "
        "Primary analytical output for workshop exercises."
    ),
    table_properties={"quality": "gold"},
)
def company_ai_investment_summary():
    return (
        spark.read.table("financial_docs_gold")
        .groupBy("company", "fiscal_period", "document_type", "ai_investment_category", "management_sentiment")
        .count()
        .withColumnRenamed("count", "document_count")
        .orderBy("company", "fiscal_period")
    )
