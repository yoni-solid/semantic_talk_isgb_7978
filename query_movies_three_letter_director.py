"""
Query isgb_scraper: What movies were directed by someone with a three-letter first name?
Uses the same Snowflake connection pattern as database.py (env credentials).
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

    # See how director names are stored (e.g. "First Last" vs "Last, First")
    print("=== Sample director names (DIR_REF) ===")
    cur.execute("""
        SELECT DIR_CD, DIR_NM FROM ISGB_SCRAPER.SUPPLY_CHAIN.DIR_REF LIMIT 15
    """)
    for row in cur.fetchall():
        print(f"  {row[0]}: {row[1]!r}")

    # Movies directed by someone whose first name has exactly 3 letters.
    # Assume format "FirstName LastName" -> first name = SPLIT_PART(DIR_NM, ' ', 1)
    print("\n=== Movies directed by someone with a three-letter first name ===")
    cur.execute("""
        SELECT m.MEDIA_TTL, d.DIR_NM
        FROM ISGB_SCRAPER.SUPPLY_CHAIN.MEDIA_MAST m
        JOIN ISGB_SCRAPER.SUPPLY_CHAIN.DIR_REF d ON m.DIR_CD = d.DIR_CD
        WHERE LEN(TRIM(SPLIT_PART(d.DIR_NM, ' ', 1))) = 3
        ORDER BY m.MEDIA_TTL
    """)
    rows = cur.fetchall()
    for row in rows:
        print(f"  {row[0]!r} (director: {row[1]!r})")
    print(f"\nTotal: {len(rows)} movies")

    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
