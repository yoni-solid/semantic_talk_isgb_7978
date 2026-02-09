"""
Query isgb_scraper: What books are most liked?
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

    # Books live in BK_CATALOG; RTG_VAL is the rating. Check distribution.
    print("=== Rating (RTG_VAL) distribution in BK_CATALOG ===")
    cur.execute("""
        SELECT RTG_VAL, COUNT(*) AS cnt
        FROM ISGB_SCRAPER.SUPPLY_CHAIN.BK_CATALOG
        GROUP BY RTG_VAL
        ORDER BY RTG_VAL DESC
    """)
    for row in cur.fetchall():
        print(f"  Rating {row[0]}: {row[1]} books")

    # Most liked = highest RTG_VAL. Get books at max rating.
    print("\n=== Books most liked (highest rating) ===")
    cur.execute("""
        SELECT b.BK_TTL, a.AUTH_NM, b.RTG_VAL
        FROM ISGB_SCRAPER.SUPPLY_CHAIN.BK_CATALOG b
        LEFT JOIN ISGB_SCRAPER.SUPPLY_CHAIN.AUTH_REF a ON b.AUTH_CD = a.AUTH_CD
        WHERE b.RTG_VAL = (SELECT MAX(RTG_VAL) FROM ISGB_SCRAPER.SUPPLY_CHAIN.BK_CATALOG)
        ORDER BY b.BK_TTL
    """)
    rows = cur.fetchall()
    for row in rows:
        print(f"  {row[0]!r} by {row[1]!r} (rating: {row[2]})")
    print(f"\nTotal: {len(rows)} books at top rating")

    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
