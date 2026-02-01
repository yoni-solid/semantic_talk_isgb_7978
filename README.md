# Web Scraping to Snowflake Pipeline

A complete automated pipeline for scraping websites, processing data, and loading into Snowflake with a normalized star schema design. This pipeline demonstrates AI-powered web scraping and data warehousing for a hands-on Web Analytics course.

## Features

- **AI-Powered Scraping**: Uses Crawl4AI with LLM extraction strategy
- **Normalized Star Schema**: Dimension and fact tables with non-obvious naming (mimics real-world messy environments)
- **No FK Constraints**: Uses code references without foreign key constraints
- **Comprehensive Logging**: File and console logging with detailed error tracking
- **Full Test Suite**: Unit and integration tests for all components

## Project Structure

```
semantic_talk_isgb_7978/
├── main.py                 # Main orchestration script
├── scraper.py              # Web scraping with Crawl4AI
├── database.py             # Snowflake operations
├── models.py               # Pydantic data models
├── requirements.txt        # Python dependencies
├── env.example             # Environment variable template
├── .gitignore             # Git ignore rules
├── tests/                  # Test suite
│   ├── test_scraper.py
│   ├── test_database.py
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
   OPENAI_API_KEY=your_openai_api_key
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

5. Configure OpenAI API key (optional but recommended for LLM extraction):
   ```
   OPENAI_API_KEY=your_openai_api_key
   ```

## Usage

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
- **LLM Extraction**: Requires OpenAI API key for optimal extraction. Will warn if not provided but may fail on complex pages
- **Pagination**: Scrapers handle pagination automatically with configurable limits

## Logging

Logs are written to:
- File: `logs/pipeline.log` (DEBUG level)
- Console: INFO level and above

## Error Handling

The pipeline includes comprehensive error handling:
- Scraping errors are logged but don't stop the pipeline
- Snowflake connection errors are retried
- Data validation errors are caught by Pydantic models
