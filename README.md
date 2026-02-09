# Web Scraping to Snowflake Pipeline

A complete automated pipeline for scraping websites, processing data, and loading into Snowflake with a normalized star schema design. This pipeline demonstrates AI-powered web scraping and data warehousing for a hands-on Web Analytics course.

## Features

- **AI-Powered Scraping**: Uses Crawl4AI with hybrid CSS selector + LLM extraction strategy
- **Normalized Star Schema**: Dimension and fact tables with non-obvious naming (mimics real-world messy environments)
- **No FK Constraints**: Uses code references without foreign key constraints
- **Business Query Generation**: Automatically generates 200 unique business questions with validated SQL queries
- **Automated Task Scheduling**: Creates Snowflake tasks that run queries hourly indefinitely
- **Comprehensive Logging**: File and console logging with detailed error tracking
- **Full Test Suite**: Unit and integration tests for all components

## Project Structure

```
semantic_talk_isgb_7978/
├── main.py                 # Main orchestration script
├── scraper.py              # Web scraping with Crawl4AI
├── database.py             # Snowflake operations and task management
├── models.py               # Pydantic data models
├── generate_queries.py     # Generate business questions and SQL queries
├── create_tasks.py         # Create Snowflake tasks for queries
├── business_queries.json   # Generated business queries (155 successful)
├── requirements.txt        # Python dependencies
├── env.example             # Environment variable template
├── .gitignore             # Git ignore rules
├── tests/                  # Test suite
│   ├── test_scraper.py
│   ├── test_database.py
│   ├── test_database_live.py
│   ├── test_models.py
│   └── test_integration.py
├── data/                  # CSV output directory
└── logs/                  # Log files directory
```

## Installation

1. Clone the repository
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Set up environment variables:
   ```bash
   cp env.example .env
   # Edit .env with your credentials
   ```
   
   Or create `.env` manually with the following variables:
   ```
   SNOWFLAKE_ACCOUNT=your_account
   SNOWFLAKE_USER=your_user
   SNOWFLAKE_PASSWORD=your_password
   SNOWFLAKE_WAREHOUSE=your_warehouse
   SNOWFLAKE_ROLE=your_role
   SNOWFLAKE_DATABASE=your_database
   LLM_PROVIDER=openai
   OPENAI_API_KEY=your_openai_api_key
   GEMINI_API_KEY=your_gemini_api_key
   ```

4. Configure Snowflake credentials in `.env`:
   ```
   SNOWFLAKE_ACCOUNT=your_account
   SNOWFLAKE_USER=your_user
   SNOWFLAKE_PASSWORD=your_password
   SNOWFLAKE_WAREHOUSE=your_warehouse
   SNOWFLAKE_ROLE=your_role
   SNOWFLAKE_DATABASE=your_database
   ```

5. Configure LLM for extraction (optional but recommended): use **either** OpenAI or Gemini. Set `LLM_PROVIDER` to `openai` or `gemini` to force one; if unset, the pipeline uses whichever API key is set (OpenAI preferred if both are set).
   ```
   LLM_PROVIDER=openai
   OPENAI_API_KEY=your_openai_api_key
   ```
   Or for Gemini:
   ```
   LLM_PROVIDER=gemini
   GEMINI_API_KEY=your_gemini_api_key
   ```

6. **First run (browser)**: The pipeline uses Crawl4AI with Playwright. On first run, if Chromium is not installed, it will run `playwright install chromium` automatically (one-time, may take a few minutes). You can also run it manually beforehand: `python -m playwright install chromium`.

## Usage

### Running the Complete Pipeline

Run the complete pipeline:
```bash
python main.py
```

The pipeline will:
1. Scrape products from web-scraping.dev
2. Scrape books from books.toscrape.com
3. Scrape films from scrapethissite.com
4. Normalize dimension data (categories, authors, directors, actors, awards)
5. Export data to CSV files
6. Connect to Snowflake
7. Create schema and tables
8. Load data into Snowflake
9. Create analytical views
10. Verify data load

### Generating Business Queries

Generate 200 unique business questions with SQL queries:
```bash
python generate_queries.py
```

This script will:
- Generate 200 diverse business questions across Products, Books, Films, and Cross-Domain categories
- Test each query against Snowflake to ensure it returns results
- Save successful queries to `business_queries.json`
- Report statistics on successful vs failed queries

The output JSON file contains:
- Query ID, category, and business question
- Validated SQL query
- Row count returned by the query
- Success/failure status

### Creating Snowflake Tasks

Create automated Snowflake tasks for all successful queries:
```bash
python create_tasks.py
```

This script will:
- Load successful queries from `business_queries.json`
- Create a Snowflake task for each query
- Schedule tasks to run hourly (`USING CRON 0 * * * * UTC`)
- Automatically resume all tasks (they start running immediately)
- Verify all tasks were created successfully

Each task is named using the pattern: `TASK_Q{id}_{category}` (e.g., `TASK_Q1_Products`)

**Task Management Methods** (available in `database.py`):
- `create_task()`: Create a single task
- `create_all_tasks()`: Create tasks for all successful queries
- `resume_task()`: Resume a suspended task
- `suspend_task()`: Suspend a task
- `drop_task()`: Drop a task
- `list_tasks()`: List all tasks in the schema

## Database Schema

### Dimension Tables (Reference Data)
- `CAT_REF`: Product categories
- `BK_CAT_REF`: Book categories
- `AUTH_REF`: Authors
- `DIR_REF`: Directors
- `PERF_REF`: Performers/Actors
- `AWARD_REF`: Award categories

### Fact Tables (Transactional Data)
- `INV_MAST`: Inventory master (products)
- `BK_CATALOG`: Book catalog
- `MEDIA_MAST`: Media master (films)

### Bridge Tables (Many-to-Many)
- `BK_CAT_XREF`: Book to category relationships
- `MEDIA_PERF_XREF`: Media to performer relationships
- `MEDIA_AWD_XREF`: Media to award relationships

### Detail Tables
- `INV_VAR`: Product variants
- `INV_REV`: Product reviews
- `INV_SIM`: Similar products

### Analytical Views
- `VW_INV_ANALYTICS`: Aggregated inventory metrics
- `VW_BK_ANALYTICS`: Aggregated book metrics
- `VW_MEDIA_ANALYTICS`: Aggregated media metrics

## Testing

Run the test suite:
```bash
pytest tests/ -v
```

## Notes

- **No FK Constraints**: The schema uses code references (cat_cd, auth_cd, etc.) but does not enforce foreign key constraints, mimicking real-world messy environments
- **Non-Obvious Naming**: Table and column names use abbreviations (INV_MAST, cat_cd, unit_prc) that require understanding to interpret
- **Hybrid Scraping**: Uses CSS selectors for initial extraction, falling back to LLM extraction when needed. This reduces token usage while extracting more data
- **LLM Extraction**: Supports OpenAI or Google Gemini (set `LLM_PROVIDER` and the corresponding API key). Will warn if no key is provided but may fail on complex pages
- **Pagination**: Scrapers handle pagination automatically with configurable limits
- **Query Generation**: Generated queries are tested against actual data to ensure they return results
- **Task Scheduling**: All tasks run hourly indefinitely. Tasks can be suspended/resumed individually using the database manager methods

## Logging

Logs are written to:
- File: `logs/pipeline.log` (DEBUG level) - Main pipeline execution
- File: `logs/task_creation.log` (INFO level) - Task creation process
- Console: INFO level and above

## Error Handling

The pipeline includes comprehensive error handling:
- Scraping errors are logged but don't stop the pipeline
- Snowflake connection errors are retried
- Data validation errors are caught by Pydantic models
