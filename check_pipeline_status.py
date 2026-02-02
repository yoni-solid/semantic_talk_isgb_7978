#!/usr/bin/env python3
"""Check pipeline status and data quality in Snowflake"""
import sys
from database import SnowflakeManager
from dotenv import load_dotenv

load_dotenv()

def main():
    db_manager = SnowflakeManager()
    try:
        db_manager.connect()
        
        # Check data counts
        verification = db_manager.verify_data()
        
        print("\n" + "="*60)
        print("SNOWFLAKE DATA QUALITY CHECK")
        print("="*60)
        
        total_rows = 0
        for table, count in verification.items():
            print(f"{table:25s}: {count:6d} rows")
            total_rows += count
        
        print("-"*60)
        print(f"{'TOTAL':25s}: {total_rows:6d} rows")
        print("="*60)
        
        # Check for garbage data
        print("\nChecking for garbage data...")
        
        # Check products for "unknown" categories
        db_manager.cursor.execute("SELECT COUNT(*) FROM SUPPLY_CHAIN.INV_MAST WHERE INV_NM LIKE '%unknown%' OR INV_NM = ''")
        unknown_products = db_manager.cursor.fetchone()[0]
        print(f"Products with 'unknown' in name or empty: {unknown_products}")
        
        # Check books for "Unknown" authors
        db_manager.cursor.execute("""
            SELECT COUNT(*) FROM SUPPLY_CHAIN.BK_CATALOG b
            JOIN SUPPLY_CHAIN.AUTH_REF a ON b.AUTH_CD = a.AUTH_CD
            WHERE a.AUTH_NM LIKE '%Unknown%' OR a.AUTH_NM = ''
        """)
        unknown_authors = db_manager.cursor.fetchone()[0]
        print(f"Books with 'Unknown' authors: {unknown_authors}")
        
        # Check films for "Unknown" directors
        db_manager.cursor.execute("""
            SELECT COUNT(*) FROM SUPPLY_CHAIN.MEDIA_MAST m
            JOIN SUPPLY_CHAIN.DIR_REF d ON m.DIR_CD = d.DIR_CD
            WHERE d.DIR_NM LIKE '%Unknown%' OR d.DIR_NM = ''
        """)
        unknown_directors = db_manager.cursor.fetchone()[0]
        print(f"Films with 'Unknown' directors: {unknown_directors}")
        
        # Sample some data
        print("\nSample Products:")
        db_manager.cursor.execute("SELECT INV_NM, UNIT_PRC, CAT_CD FROM SUPPLY_CHAIN.INV_MAST LIMIT 5")
        for row in db_manager.cursor.fetchall():
            print(f"  - {row[0]}: ${row[1]:.2f} (Category: {row[2]})")
        
        print("\nSample Books:")
        db_manager.cursor.execute("""
            SELECT b.BK_TTL, b.UNIT_PRC, a.AUTH_NM 
            FROM SUPPLY_CHAIN.BK_CATALOG b
            JOIN SUPPLY_CHAIN.AUTH_REF a ON b.AUTH_CD = a.AUTH_CD
            LIMIT 5
        """)
        for row in db_manager.cursor.fetchall():
            print(f"  - {row[0]}: ${row[1]:.2f} by {row[2]}")
        
        print("\nSample Films:")
        db_manager.cursor.execute("""
            SELECT m.MEDIA_TTL, m.YR_VAL, d.DIR_NM 
            FROM SUPPLY_CHAIN.MEDIA_MAST m
            JOIN SUPPLY_CHAIN.DIR_REF d ON m.DIR_CD = d.DIR_CD
            LIMIT 5
        """)
        for row in db_manager.cursor.fetchall():
            print(f"  - {row[0]} ({row[1]}) directed by {row[2]}")
        
        if total_rows > 0:
            print(f"\n✓ Pipeline completed with {total_rows} total rows")
            return 0
        else:
            print("\n✗ No data found - pipeline may have failed")
            return 1
            
    except Exception as e:
        print(f"Error checking pipeline: {e}")
        return 1
    finally:
        db_manager.disconnect()

if __name__ == "__main__":
    sys.exit(main())
