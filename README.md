# Robust AI Data Platforms Workshop


|              |                                                                             |
| -------------- | ----------------------------------------------------------------------------- |
| **Format**   | Two 2-hour instructor-led sessions with hands-on exercises                  |
| **Dataset**  | SEC filings & earnings materials for Apple, Amazon, Microsoft, NVIDIA, Meta |
| **Platform** | Databricks (Unity Catalog, Lakeflow Pipelines, Agent Bricks, Genie)         |

---

## Scenario

Participants build a **Financial Intelligence Platform** — an AI-powered system that ingests SEC
filings and earnings documents, extracts structured insights, and makes them queryable via natural
language, vector semantic search, knowledge graph, and finally - a hybrid context.

This is a pattern you might deploy for a client building an internal competitive intelligence or document analysis capability.

---

## Session 1 — Design and build governed and scalable data platforms (~2 hrs)

> **Pre-session:** Participants run `00_setup_participant.ipynb` before the formal session begins to create their personal schema and verify data access.

| # | Module | Duration | Artifacts Created |
|---|---|---|---|
| 00 | `00_setup_participant` | 5 min | Personal schema, personal `financial_docs` volume |
| 01 | `01_lecture_reference` *(reference only)* | — | — |
| 02 | `02_governance_as_code` | 35 min | `my_analysis` table (tagged, with lineage) |
| 03 | `03_unstructured_pipelines` | 45 min | `my_bronze`, `my_silver` tables |
| 04 | `04_codifying_processes` | 20 min | — (UI exploration + SDK read calls) |
| | Total | ~105 min | |

> `01_lecture_reference` is a self-study notebook. Participants are encouraged to read it outside the scheduled session time.

**What participants leave with:**
- How the Databricks medallion architecture extends to multi-modal (unstructured) data — binary files in Volumes, parsed text in Delta
- How Unity Catalog provides governance automatically: lineage, tagging, data quality expectations, and access control — all without manual documentation
- How `ai_parse_document`, `ai_extract`, `ai_analyze_sentiment`, and `ai_classify` compose into a SQL-native document intelligence pipeline
- How Declarative Automation Bundles (DABs) and LakeFlow Jobs turn ad-hoc notebook work into version-controlled, observable business processes

### Session 1 error-prone areas

| Risk | Where | Mitigation |
|---|---|---|
| `ai_parse_document` takes 1–3 min for a single document — participants may run it on more files and stall | `03` — PDF parsing cell | Instruct participants to run the cell exactly as written; the pre-built pipeline handles the full corpus |
| `financial_intelligence_job` not found — SDK cells fail silently if the DAB was never deployed to prod | `04` — SDK job lookup | Deploy the bundle to the `prod` target and confirm the job appears in the Jobs UI before the workshop |
| Schema name surprises — email prefixes with dots, hyphens, or `+tag` suffixes produce unexpected schema names | `00` — config derivation | Have participants read the printed schema name from `00_setup_participant` before proceeding to Lab 1 |
| Catalog name mismatch — `config.ipynb` defaults to `vectorcatalog01`; the instructor setup widget defaults to `platform-workshop` | All notebooks | Verify the catalog name in `config.ipynb` matches the actual catalog before running any setup steps |

---

## Session 2 — Semantic, graph, and contextual data layers for AI (~2 hrs)

| # | Module | Duration | Artifacts Created |
|---|---|---|---|
| 01 | `01_lecture_reference` *(reference only)* | — | — |
| 02 | `02_vector_semantic_indexing` | 35 min | — (queries against shared VS index and KA) |
| 03 | `03_knowledge_graphs` | 35 min | `triplets_raw`, `entities`, `relationships`, `entity_edges` tables |
| 04 | `04_hybrid_context_layers` | 25 min | — (queries across all three layers) |
| | Total | ~95 min | |

> `01_lecture_reference` is a self-study notebook. Use the first 20 min as discussion time or allow participants to read it independently.

**What participants leave with:**
- When to use semantic (vector), graph, and structured (metadata) retrieval — and why no single layer is sufficient for complex analytical questions
- How `ai_parse_document` → `ai_prep_search` → Vector Search compose into a production RAG pipeline without custom chunking code
- How to build a knowledge graph from financial documents using `ai_query` with structured output, stored as queryable Delta tables
- How Genie makes the knowledge graph accessible to non-technical analysts without writing SQL
- How hybrid context assembly — combining all three layers — produces richer, more accurate AI responses than any single retrieval strategy

### Session 2 error-prone areas

| Risk | Where | Mitigation |
|---|---|---|
| `ai_query` triplet extraction runs on 50+ earnings transcripts — can take 15–30+ min and consumes significant FMaaS tokens; cold serverless compute may time out | `03` — triplet extraction cell | Warn participants before running; confirm serverless compute is warm and the workspace has sufficient FMaaS quota |
| Knowledge Assistant not accessible — no programmatic way to grant KA permissions to `all workspace users` | `02` — KA query cells | Create a workshop group, add all participants, and grant `CAN MANAGE` on `financial-intelligence-assistant` before the session |
| Graph Explorer URL is hardcoded in the notebook and must be updated per deployment | `04` — Graph Explorer link | Update the URL in `04_hybrid_context_layers.ipynb` and verify all participants can reach it before the session |
| `financial_docs_for_search` not populated — Vector Search index has no data if the job's `build_search_table` task never ran | `02` — VS index query | Confirm the table exists with rows before creating the VS index (Step 6 of instructor setup) |

---

## Repository Structure

```
instructor/
├── artifacts/
│   ├── financial_docs/               # Full PDF corpus (copied to shared Volume in setup)
│   └── samples/                      # 2–3 sample PDFs (for financial_docs_sample Volume)
└── scripts/
    └── 00_instructor_setup.ipynb     # One-time setup — run before the workshop

participant/
├── bundles/
│   └── financial_pipelines/               # Declarative Automation Bundle
│       ├── databricks.yml                 # Bundle root: variables, targets, includes
│       ├── resources/
│       │   ├── financial_pipeline.yml     # Spark Declarative Pipeline definition
│       │   └── orchestration_job.yml      # LakeFlow Job: pipeline → search → validate → notify
│       ├── src/
│       │   └── financial_intelligence_pipeline/transformations/
│       │       ├── bronze_ingest.py       # Bronze: Auto Loader → raw PDFs in Delta
│       │       ├── silver_extract.py      # Silver: ai_parse_document + ai_extract
│       │       └── gold_classify.py       # Gold: ai_classify + aggregated summary
│       ├── jobs/
│       │   ├── build_search_table.ipynb   # Job task: ai_prep_search() → financial_docs_for_search
│       │   ├── validate_quality.ipynb     # Job task: data quality assertions
│       │   └── notify_complete.ipynb      # Job task: completion summary
│       └── tests/                         # Unit tests (pytest) for CI/CD
├── session1/
│   ├── 00_setup_participant.ipynb         # Participant setup: create schema + verify access
│   ├── 01_lecture_reference.ipynb         # Reference notes (self-study, not presented)
│   ├── 02_governance_as_code.ipynb        # Lab 1: Unity Catalog governance & lineage
│   ├── 03_unstructured_pipelines.ipynb   # Lab 2: AI functions + pipeline exploration
│   └── 04_codifying_processes.ipynb       # Lab 3: DAB review + SDK job control
├── session2/
│   ├── 01_lecture_reference.ipynb         # Reference notes (self-study, not presented)
│   ├── 02_vector_semantic_indexing.ipynb  # Lab 1: Agent Bricks KA + Vector Search
│   ├── 03_knowledge_graphs.ipynb          # Lab 2: Triplet extraction + Genie
│   └── 04_hybrid_context_layers.ipynb     # Lab 3: Hybrid context query + Graph Explorer
└── utils/
    ├── config.ipynb                        # Shared config — sourced via %run by all notebooks
    └── _resources/                         # Images referenced by lab notebooks

README.md
```

---

## Instructor Setup

1. Ensure the target catalog (e.g., `vectorcatalog01`) exists in the workspace
2. **Verify the catalog name** in `participant/utils/config.ipynb` matches the actual catalog — the default hardcoded value is `vectorcatalog01`. The instructor setup notebook has a widget defaulting to `platform-workshop`. These must agree before running any setup steps.
3. Create a Git Folder at the root of the **Shared** workspace folder and clone this repo
4. Run `instructor/scripts/00_instructor_setup.ipynb` (requires WORKSPACE ADMIN):
   - Creates shared schema and Volumes
   - Grants permissions to `account users`
   - Copies artifacts to shared volume
   - Guides you through deploying the DAB (pipeline + job) to the `prod` target
   - Runs the job incrementally, per the in-notebook instructions (run once per document batch — **expect 60+ min total**)
   - For Session 2: Creates Vector Search endpoint and Delta Sync index (**allow 30–60 min for initial sync**)
   - For Session 2: Creates the Agent Bricks Knowledge Assistant
   - For Session 2: Manually deploy the Graph Explorer app and update its URL in `session2/04_hybrid_context_layers.ipynb`
5. **Manually grant permissions** (cannot be automated):
   - Create a workshop participant group; grant `CAN MANAGE` on the `financial-intelligence-assistant` Knowledge Assistant
   - Grant `CAN USE` on the Vector Search endpoint to `account users`
   - Grant `CAN USE` on the Graph Explorer app to all workspace users
   - Grant `CAN USE` on the SQL warehouse to the Graph Explorer app service principal

### Pre-workshop Checklist

**Session 1 (required)**
- [ ] Target catalog exists and catalog name is consistent in `config.ipynb` and instructor setup
- [ ] `00_shared` schema exists in the target catalog
- [ ] `financial_docs_raw` volume contains PDFs for all 5 companies (~125 PDFs)
- [ ] `financial_docs_bronze`, `_silver`, `_gold`, `company_ai_investment_summary`, and `financial_docs_for_search` tables exist and have rows
- [ ] `financial_intelligence_pipeline` and `financial_intelligence_job` exist in LakeFlow Jobs with at least 5 run history entries
- [ ] `account users` have `USE CATALOG`, `USE SCHEMA`, `SELECT`, `READ VOLUME`

**Session 2 (required only if running Session 2)**
- [ ] Vector Search endpoint `financial-docs-vs-endpoint` is `ONLINE`
- [ ] Vector Search index `financial_docs_for_search_index` is synced (`ready=True`, 4,000+ chunks)
- [ ] Knowledge Assistant `financial-intelligence-assistant` responds to a test question
- [ ] Workshop participant group has `CAN MANAGE` on the Knowledge Assistant
- [ ] Graph Explorer app is deployed and accessible; URL updated in `04_hybrid_context_layers.ipynb`
- [ ] Graph Explorer app service principal has `CAN USE` on the SQL warehouse

### Participant Setup (run at the start of each session)

Each participant opens `session1/00_setup_participant.ipynb` from the Git Folder in the Workspace and runs all cells. This creates their personal schema (`<catalog>.<sanitized_username>`), a personal `financial_docs` Volume, and verifies access to the shared data and pipeline outputs.

---

## Participant Permissions Required


| Permission             | Resource                                            |
| ------------------------ | ----------------------------------------------------- |
| `USE CATALOG`          | `<catalog>`                                         |
| `CREATE SCHEMA`        | `<catalog>` (to create personal schema)             |
| `USE SCHEMA`, `SELECT` | `<catalog>.00_shared`                               |
| `READ VOLUME`          | `<catalog>.00_shared.financial_docs_raw`            |
| `READ VOLUME`          | `<catalog>.00_shared.financial_docs_sample`         |

All granted via `GRANT ... TO 'account users'` in the instructor setup notebook.

---

## Financial Document Corpus

Source: Google Drive folder `Agent Bricks FINS Data`
(access via instructor — not publicly linked)


| Company              | Documents included                                                      |
| ---------------------- | ------------------------------------------------------------------------- |
| **Apple (AAPL)**     | 10-K (2022–2024), 10-Q (Q1–Q3 2024), Q1–Q3 earnings releases         |
| **Amazon (AMZN)**    | 10-K, 10-Q (5 filings), 2023–2024 Annual Reports, Q1–Q4 2024 earnings |
| **Microsoft (MSFT)** | FY25Q4 10-K, FY25 Q1–Q3 10-Qs, FY25 Q1–Q4 press releases              |
| **NVIDIA (NVDA)**    | 10-K, 10-Q (4+), FY25 quarterly presentations + CFO commentary          |
| **Meta (META)**      | 10-K, 10-Q (5), Q3–Q4 2024 + Q1–Q3 2025 earnings call transcripts     |

Total: ~125 PDFs across 5 companies and multiple document types

---

## Extending to Session 2

Session 2 is **optional** — the workshop is complete after Session 1. If you are extending:

1. Session 2 builds directly on the Silver and Gold tables produced in Session 1
2. The Knowledge Assistant requires the pipeline to have run at least once and the Vector Search index to be synced
3. The triplet extraction in Lab 2 writes to each participant's personal schema — no additional instructor setup required beyond the Agent Bricks KA and Graph Explorer app

For teams continuing this work after the workshop, the suggested path is:

1. Add triplet extraction as a Gold+ table in the bundle pipeline
2. Create a scheduled DAB deployment that runs weekly after earnings release dates
3. Wrap `hybrid_context_query()` in a Databricks App (FastAPI + React) for analyst self-service
4. Evaluate retrieval quality using `mlflow.genai.evaluate()` with a curated question set

---

## Data Architecture

```
╔══════════════════════════════════════════════════════════════════════════════════╗
║  /Volumes/<catalog>/00_shared/financial_docs_raw/                                ║
║  ┌──────────┐  ┌──────────┐  ┌───────────┐  ┌────────┐  ┌──────┐                ║
║  │  Apple/  │  │ Amazon/  │  │Microsoft/ │  │NVIDIA/ │  │Meta/ │  ~125 PDFs     ║
║  └──────────┘  └──────────┘  └───────────┘  └────────┘  └──────┘                ║
╚══════════════════════════╤═══════════════════════════════════════════════════════╝
                           │ Auto Loader (cloudFiles, binaryFile)
╔══ SPARK DECLARATIVE PIPELINE (financial_intelligence_pipeline) ══════════════════╗
║                          ▼                                                       ║
║  ┌───────────────────────────────────────────────────────────┐                   ║
║  │  financial_docs_bronze                  Streaming Table   │                   ║
║  │  source_path · content · file_size_bytes · ingested_at    │                   ║
║  └───────────────────────────┬───────────────────────────────┘                   ║
║                              │  ai_parse_document()  +  ai_extract()             ║
║                              ▼                                                   ║
║  ┌───────────────────────────────────────────────────────────┐                   ║
║  │  financial_docs_silver                  Streaming Table   │                   ║
║  │  source_path · parsed_document · plain_text · company     │                   ║
║  │  fiscal_period · document_type · revenue · net_income     │                   ║
║  │  ✓ expect_or_drop: has_content                            │                   ║
║  │  ✓ expect: has_company · has_doc_type · has_fiscal_period │                   ║
║  └───────┬───────────────────────────────────────────────────┘                   ║
║          │  ai_classify() + ai_analyze_sentiment()                               ║
║          ▼                                                                       ║
║  ┌───────────────────────────────────────────────────────────┐                   ║
║  │  financial_docs_gold                 Materialized View    │                   ║
║  │  + ai_investment_category · management_sentiment          │                   ║
║  └───────┬───────────────────────────────────────────────────┘                   ║
║          │  GROUP BY company, fiscal_period, ai_investment_category              ║
║          ▼                                                                       ║
║  ┌───────────────────────────────────────────────────────────┐                   ║
║  │  company_ai_investment_summary       Materialized View    │                   ║
║  │  company · fiscal_period · ai_investment_category         │                   ║
║  │  document_count                                           │                   ║
║  └───────────────────────────────────────────────────────────┘                   ║
╚══════════════════════════╤═══════════════════════════════════════════════════════╝
                           │ spark.read.table("financial_docs_silver")
╔══ LAKEFLOW JOB ─ build_search_table task ════════════════════════════════════════╗
║                          ▼                                                       ║
║  ┌───────────────────────────────────────────────────────────┐                   ║
║  │  financial_docs_for_search          Managed Delta Table   │                   ║
║  │  chunk_id (PK) · chunk_to_embed · chunk_text              │                   ║
║  │  source_path · company · fiscal_period · document_type    │                   ║
║  │  delta.enableChangeDataFeed = true                        │                   ║
║  │  Produced by ai_prep_search() for semantic chunking       │                   ║
║  └───────────────────────────────────────────────────────────┘                   ║
╚══════════════════════════╤═══════════════════════════════════════════════════════╝
                           │ Delta Sync  (embedding: databricks-gte-large-en)
╔══ VECTOR SEARCH ═════════╪═══════════════════════════════════════════════════════╗
║                          ▼                                                       ║
║  ┌───────────────────────────────────────────────────────────┐                   ║
║  │  financial_docs_for_search_index      VS Delta Sync Index │                   ║
║  │  endpoint: financial-docs-vs-endpoint                     │                   ║
║  │  ~4,000+ semantic chunks                                  │                   ║
║  │  filters: company · fiscal_period · document_type         │                   ║
║  └───────────────────────┬───────────────────────────────────┘                   ║
║                          │                                                       ║
║  ┌───────────────────────▼───────────────────────────────────┐                   ║
║  │  Agent Bricks Knowledge Assistant                         │                   ║
║  │  financial-intelligence-assistant                         │                   ║
║  └───────────────────────────────────────────────────────────┘                   ║
╚══════════════════════════════════════════════════════════════════════════════════╝

╔══ KNOWLEDGE GRAPH  (Session 2 — participant schema, extracted from silver) ══════╗
║                                                                                  ║
║  financial_docs_silver (earnings transcripts) ── ai_query() ──► triplet extract  ║
║                                                          │                       ║
║              ┌───────────────────────────────────────────┼───────────────┐       ║
║              ▼                                           ▼               ▼       ║
║  ┌───────────────────────┐  ┌──────────────────────────┐  ┌─────────────────────┐║
║  │  entities             │  │  relationships           │  │  entity_edges       │║
║  │  entity_name · UID    │  │  subject · predicate     │  │  (Graph Explorer    │║
║  │  entity_type          │  │  object · company        │  │   adjacency table)  │║
║  │  description          │  │  fiscal_period           │  │                     │║
║  │  company              │  │  (partners_with,         │  │                     │║
║  │  fiscal_period        │  │   invests_in,            │  │                     │║
║  └───────────────────────┘  │   uses_technology, ...)  │  └─────────────────────┘║
║                             └──────────────────────────┘                        ║
╚══════════════════════════════════════════════════════════════════════════════════╝
```
