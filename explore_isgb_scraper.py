"""
Explore isgb_scraper database in Snowflake and answer: How many books are still in stock?
Uses the same connection pattern as database.py (env credentials).
"""
import os
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()

def get_conn():
    account = os.getenv("SNOWFLAKE_ACCOUNT")
    if account and ".snowflakecomputing.com" in account:
        account = account.replace(".snowflakecomputing.com", "")
    return snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=account,
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        role=os.getenv("SNOWFLAKE_ROLE"),
        database="ISGB_SCRAPER",
    )

def main():
    conn = get_conn()
    cur = conn.cursor()

    # 1) List schemas in ISGB_SCRAPER
    print("=== Schemas in ISGB_SCRAPER ===")
    cur.execute("SHOW SCHEMAS IN DATABASE ISGB_SCRAPER")
    for row in cur.fetchall():
        print(row)

    # 2) Use INFORMATION_SCHEMA to list tables/views (try common schema names)
    cur.execute("USE DATABASE ISGB_SCRAPER")
    cur.execute("SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE CATALOG_NAME = 'ISGB_SCRAPER'")
    schemas = [r[0] for r in cur.fetchall()]
    print("\n=== Schemas (from INFORMATION_SCHEMA) ===", schemas)

    for schema in schemas:
        cur.execute(f"SHOW TABLES IN ISGB_SCRAPER.{schema}")
        tables = cur.fetchall()
        if tables:
            print(f"\n=== Tables/Views in ISGB_SCRAPER.{schema} ===")
            for t in tables:
                print(t)
        cur.execute(f"SHOW VIEWS IN ISGB_SCRAPER.{schema}")
        views = cur.fetchall()
        if views:
            print(f"\n=== Views in ISGB_SCRAPER.{schema} ===")
            for v in views:
                print(v)

    # 3) Get columns for tables that might be book-related
    cur.execute("""
        SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE
        FROM ISGB_SCRAPER.INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_CATALOG = 'ISGB_SCRAPER'
        ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION
    """)
    print("\n=== All columns (table -> columns) ===")
    current_table = None
    cols = []
    for row in cur.fetchall():
        schema_name, table_name, col_name, data_type = row[0], row[1], row[2], row[3]
        key = f"{schema_name}.{table_name}"
        if key != current_table:
            if current_table:
                print(f"  {current_table}: {cols}")
            current_table = key
            cols = []
        cols.append(f"{col_name}({data_type})")
    if current_table:
        print(f"  {current_table}: {cols}")

    # 4) BK_CATALOG has AVAIL_STS — inspect values and count books in stock
    print("\n=== Distinct AVAIL_STS in SUPPLY_CHAIN.BK_CATALOG ===")
    cur.execute("""
        SELECT AVAIL_STS, COUNT(*) AS cnt
        FROM ISGB_SCRAPER.SUPPLY_CHAIN.BK_CATALOG
        GROUP BY AVAIL_STS
        ORDER BY cnt DESC
    """)
    for row in cur.fetchall():
        print(f"  {row[0]!r}: {row[1]}")

    print("\n=== Books still in stock (AVAIL_STS = 'In stock') ===")
    cur.execute("""
        SELECT COUNT(*) FROM ISGB_SCRAPER.SUPPLY_CHAIN.BK_CATALOG
        WHERE AVAIL_STS = 'In stock'
    """)
    in_stock = cur.fetchone()[0]
    print(f"  Count: {in_stock}")

    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
