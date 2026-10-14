# uber-taxi-agentic-etl
 
Agentic ETL pipeline for Uber Delhi NCR ride bookings, built with PySpark, Azure Databricks, ADLS Gen2, Delta Lake, LangGraph and Microsoft Foundry (GPT).
 
## 1. Overview
 
Data pipelines can finish with status "Succeeded" while still delivering duplicate rows, missing days, impossible fares or shrunken record counts. This project builds a medallion-architecture ETL pipeline for Uber taxi ride bookings and adds a multi-agent AI layer that runs the pipeline, validates the output and answers questions about the data in plain language.
 
The project has two phases:
 
- **Phase 1 - ETL pipeline:** ingest the raw booking CSV from Azure Data Lake Storage Gen2, transform it with PySpark on Databricks through Bronze, Silver and Gold Delta layers, and validate the result.
- **Phase 2 - Multi-agent system:** a LangGraph team of agents (supervisor, etl, quality, analytics, explain), powered by a GPT model on Microsoft Foundry, that routes requests, runs the pipeline, checks data quality and explains results.
 
## 2. Architecture
 
```
Source CSV (ADLS Gen2)
        |
        v
   Bronze (raw Delta + ingestion_timestamp)
        |
        v
   Silver (typed, de-duplicated, filtered Delta)
        |
        v
   Gold (daily / location / payment summary Delta tables)
 
LangGraph agents (Foundry gpt-5-mini) call these layers as tools:
supervisor -> etl -> quality -> analytics
supervisor -> explain
```
 
| Component | Technology | Role |
|---|---|---|
| Storage | ADLS Gen2 (storage account `azstore01`, container `uber-taxi`) | Source CSV and all Delta output |
| Processing | Azure Databricks, PySpark, Delta Lake | Bronze to Silver to Gold transformations |
| Agent orchestration | LangGraph, LangChain | Stateful graph with conditional routing |
| LLM | Microsoft Foundry, `gpt-5-mini` via `AzureChatOpenAI` | Route selection, analytics answers, explanations |
| BI | Power BI (`Uber.pbix`), dashboard GIF | Business-facing view of Gold metrics |
 
## 3. Dataset
 
Uber ride bookings for Delhi NCR (cars, autos and bikes), one row per booking, delivered as `ncr_ride_bookings.csv` with 21 columns:
 
Date, Time, Booking ID, Booking Status, Customer ID, Vehicle Type, Pickup Location, Drop Location, Avg VTAT, Avg CTAT, Cancelled Rides by Customer, Reason for cancelling by Customer, Cancelled Rides by Driver, Driver Cancellation Reason, Incomplete Rides, Incomplete Rides Reason, Booking Value, Ride Distance, Driver Ratings, Customer Rating, Payment Method.
 
Data characteristics to be aware of:
 
- Missing values appear as the literal text `null` in the CSV.
- Booking ID and Customer ID values are wrapped in quotes.
- Date (dd-MM-yyyy) and Time are separate columns.
- Outcome columns depend on status: for example a "No Driver Found" booking has no value, distance or ratings, while a Completed booking carries them.
- Cancellation and incomplete flags are `1` or null, each paired with a reason column.
 
## 4. Storage layout (ADLS Gen2)
 
All paths are built in `src/config.py` from the account and container names.
 
| Layer | Path under `abfss://uber-taxi@azstore01.dfs.core.windows.net` | Format |
|---|---|---|
| Source | `/ncr_ride_bookings.csv` | CSV |
| Bronze | `/output/bronze` | Delta |
| Silver | `/output/silver` | Delta |
| Gold - daily | `/output/gold/daily_trip_summary` | Delta |
| Gold - location | `/output/gold/location_trip_summary` | Delta |
| Gold - payment | `/output/gold/payment_type_trip_summary` | Delta |
 
## 5. Repository structure
 
```
uber-taxi-agentic-etl/
  data/                  sample files and test data
  logs/                  run logs
  notebooks/
    uber-etl.ipynb       install deps, run ETL, preview tables, invoke agents, plot graph
  src/
    config.py            storage account, container, paths, Foundry endpoint and model
    etl_pipeline.py      read_source, build_bronze, build_silver, build_gold, run_etl
    quality_checks.py    run_quality_checks - Silver validation
    gold_analytics.py    get_gold_metrics - totals, top locations, payment summary
    agents.py            Foundry client, ETLState, agent nodes, build_agent_graph
  requirements.txt
  .env.example
  README.md
```
 
## 6. How the pipeline works
 
### Bronze (raw ingestion)
- Reads the CSV with header and schema inference.
- Replaces spaces in column names with underscores (for example `Booking Value` becomes `Booking_Value`).
- Adds `ingestion_timestamp` and writes a Delta table with overwrite.
 
### Silver (clean and filter)
- Casts booking value, ride distance, driver rating and customer rating to double.
- Removes duplicate records.
- Keeps Completed bookings with a positive booking value and ride distance.
- Derives `pickup_date` and `pickup_hour`.
 
### Gold (business aggregates)
Three tables, each with total trips, total revenue, average booking value, average ride distance and average driver rating, grouped by:
 
- pickup date (`daily_trip_summary`)
- pickup location (`location_trip_summary`)
- payment method (`payment_type_trip_summary`)
 
`run_etl(spark)` chains all steps and returns a status dictionary with row counts for every layer.
 
## 7. Data quality
 
`run_quality_checks(spark)` loads the Silver table and returns one structured result with a deterministic `passed` or `failed` status. Checks cover row count above zero, no nulls in required columns, non-negative durations, and positive distance and amount. The pass/fail decision is made in code, never by the language model.
 
## 8. Multi-agent system
 
All agents share a typed state (`ETLState`): `query`, `route`, `etl_result`, `quality_result`, `analytics_result`, `answer`.
 
| Node | Role | Function used |
|---|---|---|
| supervisor | Asks the LLM to pick one route: `run_etl`, `quality`, `analytics` or `explain`; falls back to keyword rules if the reply is invalid | Foundry client |
| etl | Runs Bronze, Silver and Gold; returns `failed` with the error on exception | `run_etl` |
| quality | Validates Silver; returns the structured result or an error | `run_quality_checks` |
| analytics | Reads Gold tables and asks the LLM to answer using only the computed metrics | `get_gold_metrics` |
| explain | Answers conceptual questions without claiming to have run anything | Foundry client |
 
Typical paths:
 
- Full run: supervisor, etl, quality, analytics
- Validation only: supervisor, quality
- Question: supervisor, explain
 
The graph can be exported with `etl_agent_graph.get_graph().draw_mermaid_png()`.
 
### Guardrails
- Supervisor output is limited to an allow-list of four routes.
- Analytics prompt forbids invented values.
- Explain prompt forbids claiming job execution.
- Every node wraps risky work in try/except.
- Model client uses `max_retries=2` and a 60-second timeout.
 
## 9. Setup
 
### Prerequisites
- Azure subscription with a Storage Account (hierarchical namespace enabled) and Azure Databricks workspace
- A GPT model deployed on Microsoft Foundry (deployment name, endpoint, credentials)
- Databricks cluster with Delta Lake support
 
### Steps
1. Upload `ncr_ride_bookings.csv` to the `uber-taxi` container in `azstore01`.
2. Import the repository into your Databricks workspace (for example under `Users/<you>/uber-taxi-agentic-etl`).
3. Copy `.env.example` and provide your own values. Do not commit secrets.
4. Open `notebooks/uber-etl.ipynb` and attach it to a cluster.
5. Run the install cell (`%pip install ...`), then the restart cell.
6. Make sure the `sys.path.insert(...)` cell points to your `src` folder.
 
### Dependencies
```
langchain==0.3.27
langchain-core==0.3.72
langchain-community==0.3.27
langgraph==0.6.2
langchain-openai==0.3.27
```
PySpark and Delta Lake are provided by the Databricks runtime.
 
### Configuration (`.env.example`)
```
STORAGE_ACCOUNT_NAME=
STORAGE_CONTAINER_NAME=
STORAGE_ACCOUNT_KEY=        # prefer a secret scope or service principal
FOUNDRY_ENDPOINT=
FOUNDRY_MODEL=
FOUNDRY_API_KEY=            # prefer Microsoft Entra ID or a secret scope
```
 
## 10. Execution order
 
1. Install dependencies and restart Python.
2. Run the ETL: `run_etl(spark)`.
3. Preview Bronze, Silver and the three Gold tables with `show()`.
4. Build the agent graph: `build_agent_graph(spark)`.
5. Invoke it with natural-language queries, for example:
   - "Run ETL and analyze Gold metrics."
   - "Check null values and invalid records in Silver."
   - "Analyze the top pickup locations by number of trips."
   - "Explain the Bronze, Silver, and Gold layers."
6. Render the graph with `draw_mermaid_png()`.
 
## 11. Security
 
- Never commit storage keys or API keys. Use a Databricks secret scope, Azure Key Vault, a service principal or Microsoft Entra ID.
- If any key was ever stored in source code, rotate it immediately.
- The storage account has anonymous blob access disabled, TLS 1.2 minimum, secure transfer required, and 7-day soft delete for blobs and containers.
 
## 12. Current limitations and roadmap
 
This is the current implementation status:
 
- Schema is inferred, not explicit; the text `null` is not yet converted to a real null; quotes on IDs are not stripped.
- Bronze has only `ingestion_timestamp` (no `source_file` or `batch_id`); loads are full overwrite, not incremental.
- Silver keeps only Completed bookings; there is no quarantine table for rejected records, and Date and Time are not yet combined into `booking_timestamp`.
- Quality checks still reference taxi-trip column names and need to be rewritten for Uber columns (booking value, ride distance, status and flag consistency, uniqueness, timeliness, volume).
- Gold has three tables; cancellation, VTAT/CTAT, peak-hour, ratings and incomplete-ride tables are pending.
- `get_gold_metrics` orders payment rows by `payment_type`; the column is `Payment_Method`.
- Pipeline runs from a notebook; packaging as a Databricks Job with retries and alerts is pending.
- Agents are not yet integrated with the Databricks Jobs API, run history or report writing.
- The formal fault-injection campaign and evaluation metrics have not been run yet.
 
Planned work:
 
1. Explicit 21-column schema, real null handling, quarantine layer and full Silver rules.
2. Rewrite quality checks with structured results (rule, layer, status, metric, threshold, sample failures).
3. Additional Gold tables with OPTIMIZE and Z-ordering.
4. Databricks Job with retries and failure alerts.
5. LLM severity classification (critical, warning, info) in JSON, never overriding a failed hard rule.
6. Run reports written to storage, persisted run state, tool-call logging.
7. Fault-injection tests and evaluation: detection rate, false alarm rate, recovery rate, time saved, report quality, cost and latency.
 
## 13. Evaluation plan
 
| Metric | How it is measured |
|---|---|
| Detection rate | Share of injected faults correctly detected |
| False alarm rate | Healthy runs wrongly flagged |
| Recovery rate | Failures stopped at the right node and explained correctly |
| Time saved | Manual validation time versus agent-run time |
| Report quality | Manual review of correctness and clarity |
| Cost and latency | Tokens and time per run |
 
