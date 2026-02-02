#!/usr/bin/env python3
"""Update Snowflake tasks with new and updated queries"""
import sys
import json
from database import SnowflakeManager
from dotenv import load_dotenv

load_dotenv()

def main():
    db_manager = SnowflakeManager()
    try:
        db_manager.connect()
        
        # Load updated queries
        with open("business_queries.json", 'r') as f:
            data = json.load(f)
        
        # Get new queries (IDs > 200) and updated query 157
        new_queries = [q for q in data.get("queries", []) if q.get("id", 0) > 200 and q.get("status") == "success"]
        updated_query = [q for q in data.get("queries", []) if q.get("id") == 157 and q.get("status") == "success"]
        
        queries_to_add = new_queries + updated_query
        
        if not queries_to_add:
            print("No new or updated queries to add to tasks")
            return 0
        
        print(f"Updating Snowflake tasks...")
        print(f"  New queries to add: {len(new_queries)}")
        print(f"  Updated query to recreate: {len(updated_query)}")
        print("="*60)
        
        # For updated query 157, drop the old task first
        if updated_query:
            old_task_name = f"TASK_Q157_Films"
            try:
                db_manager.cursor.execute(f"DROP TASK IF EXISTS SUPPLY_CHAIN.{old_task_name}")
                print(f"✓ Dropped old task: {old_task_name}")
            except Exception as e:
                print(f"⚠️  Could not drop old task {old_task_name}: {e}")
        
        # Create tasks for new and updated queries
        created = []
        failed = []
        
        for query_data in queries_to_add:
            query_id = query_data.get("id")
            category = query_data.get("category", "Unknown")
            query_text = query_data.get("query")
            question = query_data.get("question", "")
            
            if not query_text:
                continue
            
            task_name = f"TASK_Q{query_id}_{category}"
            
            try:
                created_name = db_manager.create_task(task_name, query_text)
                created.append({
                    "id": query_id,
                    "name": created_name,
                    "category": category,
                    "question": question
                })
                print(f"✓ Created task: {created_name} (Q{query_id})")
            except Exception as e:
                failed.append({
                    "id": query_id,
                    "reason": str(e)
                })
                print(f"✗ Failed to create task for Q{query_id}: {e}")
        
        print("="*60)
        print(f"Successfully created {len(created)} tasks")
        if failed:
            print(f"Failed to create {len(failed)} tasks")
            for f in failed:
                print(f"  - Q{f['id']}: {f['reason']}")
        
        # List all tasks to verify
        print("\nCurrent tasks in Snowflake:")
        tasks = db_manager.list_tasks()
        print(f"  Total tasks: {len(tasks)}")
        if tasks:
            print("  Sample tasks:")
            for task in tasks[:10]:
                print(f"    - {task}")
            if len(tasks) > 10:
                print(f"    ... and {len(tasks) - 10} more")
        
        return 0 if not failed else 1
        
    except Exception as e:
        print(f"Error updating tasks: {e}")
        import traceback
        traceback.print_exc()
        return 1
    finally:
        db_manager.disconnect()

if __name__ == "__main__":
    sys.exit(main())
