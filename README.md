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

## Workshop Outline

### Session 1 — Design and build governed and scalable data platforms (2 hrs)


| Time       | Format          | Topic                                                                      | Notebook                             |
| ------------ | ----------------- | ---------------------------------------------------------------------------- | -------------------------------------- |
| 0:00–0:20 | Lecture         | AI data platform reference architectures & constraints                     | `session1/01_lecture_reference.md`   |
| 0:20–0:55 | Hands-on        | Data architecture & governance as code (schemas, lineage, access, quality) | `session1/02_governance_as_code`     |
| 0:55–1:40 | Hands-on        | Scalable pipelines for unstructured data (ingest, transform, embed, store) | `session1/03_unstructured_pipelines` |
| 1:40–2:00 | Hands-on/Review | Codifying data business processes (DAB review + job run)                   | `session1/04_codifying_processes`    |

**What participants leave with:**

- An understanding of the Databricks medallion architecture for multi-modal (unstructured) data
- Familiarity with Unity Catalog governance: lineage, tagging, data quality, access control
- Hands-on experience with `ai_parse_document`, `ai_extract`, and `ai_classify`
- A working mental model of Databricks Asset Bundles and LakeFlow Jobs

### Session 2 — Semantic, graph, and contextual data layers for AI (2 hrs)


| Time       | Format   | Topic                                                                   | Notebook                               |
| ------------ | ---------- | ------------------------------------------------------------------------- | ---------------------------------------- |
| 0:00–0:20 | Lecture  | Context engineering for AI systems                                      | `session2/01_lecture_reference.md`     |
| 0:20–0:55 | Hands-on | Vector databases & semantic indexing (Agent Bricks Knowledge Assistant) | `session2/02_vector_semantic_indexing` |
| 0:55–1:30 | Hands-on | Knowledge graphs for RAG & agents (triplet extraction + Genie)          | `session2/03_knowledge_graphs`         |
| 1:30–2:00 | Hands-on | Designing hybrid context layers (Graph Explorer + orchestration)        | `session2/04_hybrid_context_layers`    |

**What participants leave with:**

- Working knowledge of when to use semantic (vector), graph, and structured (metadata) retrieval
- Experience building a knowledge graph from financial documents using `ai_query`
- Hands-on experience using `ai_query`, Genie, Vector Search, and Knowledge Assistant
- An understanding of hybrid context assembly as a design pattern for production AI systems and how Databricks can be an integral part of a Knowledge Graph system

---

## Repository Structure

```
instructor/
├── artifacts/
│   └── ...
└── scripts/
    └── 00_instructor_setup.ipynb    # One-time setup — run before the workshop

participant/
├── bundles/
│   └── financial_pipelines/               # Databricks Asset Bundle
│       ├── databricks.yml                 # Bundle root: variables, targets, includes
│       ├── resources/
│       │   ├── financial_pipeline.yml     # Spark Declarative Pipeline definition
│       │   └── orchestration_job.yml      # LakeFlow Job: pipeline → validate → notify
│       ├── src/
│       │   └── financial_intelligence_pipeline/
│       │       ├── bronze_ingest.py       # Bronze: Auto Loader → raw PDFs in Delta
│       │       ├── silver_extract.py      # Silver: ai_parse_document + ai_extract
│       │       └── gold_classify.py       # Gold: ai_classify + aggregated summary
│       └── jobs/
│           ├── validate_quality.ipynb     # Job task: data quality assertions
│           └── notify_complete.ipynb      # Job task: completion summary
├── session1/
│   ├── 01_lecture_reference.md            # Reference notes for the opening lecture
│   ├── 02_governance_as_code.ipynb        # Lab 1: Unity Catalog governance & lineage
│   ├── 03_unstructured_pipelines.ipynb    # Lab 2: AI functions + pipeline exploration
│   └── 04_codifying_processes.ipynb       # Lab 3: DAB review + job trigger
├── session2/
│   ├── 01_lecture_reference.md            # Reference notes for context engineering lecture
│   ├── 02_vector_semantic_indexing.ipynb  # Lab 1: Agent Bricks KA + Vector Search
│   ├── 03_knowledge_graphs.ipynb          # Lab 2: Triplet extraction + Genie
│   └── 04_hybrid_context_layers.ipynb     # Lab 3: Graph Explorer + hybrid query
└── utils/
    └── config.ipynb                       # Shared config — sourced by all participant notebooks

README.md
SETUP-Robust-AI-data-platforms-workshop.ipynb   # Participant init — run from /Workspace/Shared
```

---

## Instructor setup

1. Ensure the `platform-workshop` catalog exists in the target workspace
2. Create Git Folder at root of SHARED workspace folder.
3. Run `instructor/scripts/00_instructor_setup.ipynb` (requires WORKSPACE ADMIN)
   - Creates shared schema and Volumes
   - Grants permissions to `account users`
   - Copies artifacts to shared volume
   - Deploy the Asset Bundle containing the Pipeline and Job
   - Run the job (do this incrementally, per the instructions, to create multiple runs.  This will take 60 min or more.)
   - For Session 2: Create vector index and KA
   - For Session 2: Install the Graph Explorer app
4. Copy `SETUP-Robust-AI-data-platforms-workshop.ipynb` to `/Workspace/shared/` in the workspace

## Participant permissions required


| Permission             | Resource                                            |
| ------------------------ | ----------------------------------------------------- |
| `USE CATALOG`          | `platform-workshop`                                 |
| `CREATE SCHEMA`        | `platform-workshop` (to create personal schema)     |
| `USE SCHEMA`, `SELECT` | `platform-workshop.00_shared`                       |
| `READ VOLUME`          | `platform-workshop.00_shared.financial_docs_raw`    |
| `READ VOLUME`          | `platform-workshop.00_shared.financial_docs_sample` |

All granted via `GRANT ... TO 'account users'` in the instructor setup notebook.

### Participant setup (run at the start of the workshop)

Each participant opens `SETUP-Robust-AI-data-platforms-workshop.ipynb` from `/Workspace/shared/` and runs all cells. This creates their personal schema (`platform-workshop.<username>`), Volume, replicates the Notebooks, and verifies access to the shared data.

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
2. The Knowledge Assistant requires the pipeline to have run at least once
3. The triplet extraction in Lab 2 writes to each participant's personal schema — no additional
   instructor setup required beyond the Agent Bricks KA and Graph Explorer app

For teams continuing this work after the workshop, the suggested path is:

1. Add triplet extraction as a Gold+ table in the bundle pipeline
2. Create a scheduled DAB deployment that runs weekly after earnings release dates
3. Wrap `hybrid_context_query()` in a Databricks App (FastAPI + React) for analyst self-service
4. Evaluate retrieval quality using `mlflow.genai.evaluate()` with a curated question set

---

## Data Architecture

```
╔══════════════════════════════════════════════════════════════════════════════════╗
║  /Volumes/platform-workshop/00_shared/financial_docs_raw/                        ║
║  ┌──────────┐  ┌──────────┐  ┌───────────┐  ┌────────┐  ┌──────┐                 ║
║  │  Apple/  │  │ Amazon/  │  │Microsoft/ │  │NVIDIA/ │  │Meta/ │  ~130 PDFs      ║
║  └──────────┘  └──────────┘  └───────────┘  └────────┘  └──────┘                 ║
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
║  │  source_path · parsed_text · company · fiscal_period      │                   ║
║  │  document_type · revenue_reported · net_income            │                   ║
║  │  ✓ expect_or_drop: has_content                            │                   ║
║  │  ✓ expect: has_company · has_doc_type                     │                   ║
║  └───────┬───────────────────────────────────────────────────┘                   ║
║          │  ai_classify()                                                        ║
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
╔══ LAKEFLOW JOB (build_search_table task) ════════════════════════════════════════╗
║                          ▼                                                       ║
║  ┌───────────────────────────────────────────────────────────┐                   ║
║  │  financial_docs_for_search          Managed Delta Table   │                   ║
║  │  source_path (PK) · parsed_text · company                 │                   ║
║  │  fiscal_period · document_type                            │                   ║
║  │  delta.enableChangeDataFeed = true                        │                   ║
║  └───────────────────────────────────────────────────────────┘                   ║
╚══════════════════════════╤═══════════════════════════════════════════════════════╝
                           │ Delta Sync  (embedding: databricks-gte-large-en)
╔══ VECTOR SEARCH ═════════╪═══════════════════════════════════════════════════════╗
║                          ▼                                                       ║
║  ┌───────────────────────────────────────────────────────────┐                   ║
║  │  financial_docs_for_search_index      VS Delta Sync Index │                   ║
║  │  endpoint: financial-docs-vs-endpoint                     │                   ║
║  │  filters: company · fiscal_period · document_type         │                   ║
║  └───────────────────────────┬───────────────────────────────┘                   ║
║                              │                                                   ║
║  ┌───────────────────────────▼───────────────────────────────┐                   ║
║  │  Agent Bricks Knowledge Assistant                         │                   ║
║  │  financial-intelligence-assistant                         │                   ║
║  └───────────────────────────────────────────────────────────┘                   ║
╚══════════════════════════════════════════════════════════════════════════════════╝

╔══ KNOWLEDGE GRAPH  (Session 2 — participant schema, extracted from silver) ══════╗
║                                                                                  ║
║  financial_docs_silver ──── ai_extract() ──► triplet extraction notebook         ║
║                                                          │                       ║
║              ┌───────────────────────────────────────────┼───────────────┐       ║
║              ▼                                           ▼               ▼       ║
║  ┌───────────────────────┐  ┌──────────────────────────┐  ┌─────────────────────┐║
║  │  entities             │  │  relationships           │  │  entity_edges       │║
║  │  entity_name          │  │  subject · predicate     │  │  src · dst          │║
║  │  entity_type          │  │  object · company        │  │  edge_type          │║
║  │  description          │  │  fiscal_period           │  │  mention_count      │║
║  │  company              │  │  (partners_with,         │  │                     │║
║  │  fiscal_period        │  │   invests_in,            │  │  (undirected        │║
║  └───────────────────────┘  │   uses_technology,       │  │   adjacency list    │║
║                             │   acquires, ...)         │  │   for traversal     │║
║                             └──────────────────────────┘  │   & Graph Explorer) │║
║                                                           └─────────────────────┘║
╚══════════════════════════════════════════════════════════════════════════════════╝
```
