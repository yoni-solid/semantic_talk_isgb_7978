#!/usr/bin/env python3
"""Check the analytical views to ensure all columns have interesting data"""
import sys
from database import SnowflakeManager
from dotenv import load_dotenv

load_dotenv()

def check_view(db_manager, view_name, expected_columns):
    """Check a view and report on column data quality"""
    print(f"\n{'='*60}")
    print(f"Checking {view_name}")
    print('='*60)
    
    try:
        # Get sample rows
        db_manager.cursor.execute(f"SELECT * FROM SUPPLY_CHAIN.{view_name} LIMIT 10")
        columns = [desc[0] for desc in db_manager.cursor.description]
        rows = db_manager.cursor.fetchall()
        
        print(f"\nColumns: {', '.join(columns)}")
        print(f"Row count: {len(rows)}")
        
        if len(rows) == 0:
            print("⚠️  WARNING: View has no rows!")
            return False
        
        # Check each column for interesting data
        issues = []
        for col_idx, col_name in enumerate(columns):
            # Get distinct values for this column
            db_manager.cursor.execute(f"""
                SELECT DISTINCT {col_name}, COUNT(*) as cnt
                FROM SUPPLY_CHAIN.{view_name}
                GROUP BY {col_name}
                ORDER BY cnt DESC
                LIMIT 5
            """)
            distinct_values = db_manager.cursor.fetchall()
            
            # Check for problematic values
            problematic = ['Not Specified', 'Unknown', 'unknown', '', None, 'NULL']
            has_problem = any(str(val[0]) in problematic for val in distinct_values if val[0] is not None)
            
            if has_problem:
                issues.append(f"  ⚠️  {col_name}: Contains problematic values")
                for val, cnt in distinct_values[:3]:
                    if val is not None and str(val) in problematic:
                        print(f"      - '{val}' appears {cnt} times")
            else:
                print(f"  ✓ {col_name}: OK")
        
        # Show sample data
        print(f"\nSample rows:")
        for i, row in enumerate(rows[:3], 1):
            print(f"  Row {i}:")
            for col_idx, col_name in enumerate(columns):
                val = row[col_idx]
                if val is None:
                    val = "NULL"
                elif isinstance(val, str) and len(val) > 50:
                    val = val[:50] + "..."
                print(f"    {col_name}: {val}")
        
        if issues:
            print(f"\n⚠️  Issues found:")
            for issue in issues:
                print(issue)
            return False
        else:
            print(f"\n✓ All columns have interesting data!")
            return True
            
    except Exception as e:
        print(f"Error checking view: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    db_manager = SnowflakeManager()
    try:
        db_manager.connect()
        
        # Check each view
        views_to_check = [
            ("VW_BK_ANALYTICS", ["BK_ID", "BK_TTL", "AUTH_NM", "UNIT_PRC", "RTG_VAL", "CATEGORIES", "CAT_CNT"]),
            ("VW_INV_ANALYTICS", ["INV_ID", "INV_NM", "CAT_NM", "UNIT_PRC", "AVG_RTG", "REV_CNT", "VAR_CNT"]),
            ("VW_MEDIA_ANALYTICS", ["MEDIA_ID", "MEDIA_TTL", "YR_VAL", "DIR_NM", "AWD_NM", "PERF_CNT", "PERFORMERS"])
        ]
        
        all_ok = True
        for view_name, expected_cols in views_to_check:
            ok = check_view(db_manager, view_name, expected_cols)
            if not ok:
                all_ok = False
        
        if all_ok:
            print("\n" + "="*60)
            print("✓ ALL VIEWS HAVE INTERESTING DATA IN ALL COLUMNS!")
            print("="*60)
            return 0
        else:
            print("\n" + "="*60)
            print("⚠️  SOME VIEWS HAVE ISSUES - NEED TO FIX")
            print("="*60)
            return 1
            
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        return 1
    finally:
        db_manager.disconnect()

if __name__ == "__main__":
    sys.exit(main())
