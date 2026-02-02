#!/usr/bin/env python3
"""Test a sample of business queries to see if they need updates"""
import json
import sys
from database import SnowflakeManager
from dotenv import load_dotenv

load_dotenv()

def test_query(db_manager, query_text, query_id, question):
    """Test a single query"""
    try:
        db_manager.cursor.execute(query_text)
        rows = db_manager.cursor.fetchall()
        row_count = len(rows)
        
        # Check if query returns meaningful data
        has_data = row_count > 0
        
        # Check for "Not Specified" or "Unknown" in results (if string query)
        has_problematic_data = False
        if rows and has_data:
            for row in rows[:5]:  # Check first 5 rows
                for val in row:
                    if val and isinstance(val, str):
                        if "Not Specified" in val or "Unknown" in val:
                            has_problematic_data = True
                            break
        
        return {
            "id": query_id,
            "question": question,
            "row_count": row_count,
            "has_data": has_data,
            "has_problematic_data": has_problematic_data,
            "status": "success" if has_data else "no_data"
        }
    except Exception as e:
        return {
            "id": query_id,
            "question": question,
            "row_count": 0,
            "has_data": False,
            "has_problematic_data": False,
            "status": "error",
            "error": str(e)
        }

def main():
    db_manager = SnowflakeManager()
    try:
        db_manager.connect()
        
        # Load queries
        with open("business_queries.json", 'r') as f:
            data = json.load(f)
        
        queries = data.get("queries", [])
        successful_queries = [q for q in queries if q.get("status") == "success"]
        
        print(f"Testing {len(successful_queries)} successful queries...")
        print("="*60)
        
        # Test a sample of queries that might be affected
        # Focus on queries about authors, directors, performers, variants, reviews
        test_categories = ["Books", "Films", "Cross-Domain"]
        queries_to_test = [
            q for q in successful_queries 
            if any(cat in q.get("category", "") for cat in test_categories)
            or "AUTH" in q.get("query", "").upper()
            or "DIR" in q.get("query", "").upper()
            or "PERF" in q.get("query", "").upper()
            or "VAR" in q.get("query", "").upper()
            or "REV" in q.get("query", "").upper()
        ]
        
        # Also test a random sample of other queries
        import random
        other_queries = [q for q in successful_queries if q not in queries_to_test]
        random_sample = random.sample(other_queries, min(20, len(other_queries)))
        queries_to_test.extend(random_sample)
        
        results = []
        issues = []
        
        for query_data in queries_to_test:
            query_id = query_data.get("id")
            question = query_data.get("question", "")
            query_text = query_data.get("query", "")
            
            if not query_text:
                continue
            
            result = test_query(db_manager, query_text, query_id, question)
            results.append(result)
            
            if result["status"] == "error":
                issues.append(f"Query {query_id}: ERROR - {result.get('error', 'Unknown error')}")
            elif result["status"] == "no_data":
                issues.append(f"Query {query_id}: No data returned - '{question[:50]}...'")
            elif result["has_problematic_data"]:
                issues.append(f"Query {query_id}: Contains 'Not Specified' or 'Unknown' - '{question[:50]}...'")
        
        # Summary
        print(f"\nTested {len(results)} queries")
        print(f"  Successful: {sum(1 for r in results if r['status'] == 'success')}")
        print(f"  No data: {sum(1 for r in results if r['status'] == 'no_data')}")
        print(f"  Errors: {sum(1 for r in results if r['status'] == 'error')}")
        print(f"  With problematic data: {sum(1 for r in results if r.get('has_problematic_data'))}")
        
        if issues:
            print(f"\n⚠️  Found {len(issues)} potential issues:")
            for issue in issues[:20]:  # Show first 20
                print(f"  - {issue}")
            if len(issues) > 20:
                print(f"  ... and {len(issues) - 20} more")
        else:
            print("\n✓ All tested queries are working correctly!")
        
        return 0 if not issues else 1
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        return 1
    finally:
        db_manager.disconnect()

if __name__ == "__main__":
    sys.exit(main())
