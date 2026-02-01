"""
Snowflake database operations with star schema design.
Uses non-obvious table names and no FK constraints to mimic real-world messy environments.
"""
import logging
import os
from typing import Dict, Optional
import pandas as pd
from dotenv import load_dotenv
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

load_dotenv()

logger = logging.getLogger(__name__)


class SnowflakeManager:
    """Manages Snowflake connections and operations"""
    
    def __init__(self):
        self.account = os.getenv("SNOWFLAKE_ACCOUNT")
        self.user = os.getenv("SNOWFLAKE_USER")
        self.password = os.getenv("SNOWFLAKE_PASSWORD")
        self.warehouse = os.getenv("SNOWFLAKE_WAREHOUSE")
        self.role = os.getenv("SNOWFLAKE_ROLE")
        self.database = os.getenv("SNOWFLAKE_DATABASE", "SUPPLY_CHAIN_DB")
        self.schema = "SUPPLY_CHAIN"
        self.conn = None
        self.cursor = None

    def connect(self):
        """Establish Snowflake connection"""
        try:
            # Clean account identifier - remove .snowflakecomputing.com if present
            account = self.account
            if account and '.snowflakecomputing.com' in account:
                account = account.replace('.snowflakecomputing.com', '')
                logger.info(f"Cleaned account identifier: {account}")
            
            self.conn = snowflake.connector.connect(
                user=self.user,
                password=self.password,
                account=account,
                warehouse=self.warehouse,
                role=self.role,
                database=self.database
            )
            self.cursor = self.conn.cursor()
            logger.info("Successfully connected to Snowflake")
        except Exception as e:
            logger.error(f"Failed to connect to Snowflake: {str(e)}")
            raise

    def disconnect(self):
        """Close Snowflake connection"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        logger.info("Disconnected from Snowflake")

    def create_schema(self):
        """Create SUPPLY_CHAIN schema"""
        try:
            # Create database if specified and doesn't exist
            if self.database:
                try:
                    self.cursor.execute(f"CREATE DATABASE IF NOT EXISTS {self.database}")
                    logger.info(f"Database {self.database} ready")
                except Exception as e:
                    logger.warning(f"Could not create database {self.database}: {str(e)}")
                
                # Use database
                try:
                    self.cursor.execute(f"USE DATABASE {self.database}")
                except Exception as e:
                    logger.warning(f"Could not use database {self.database}, continuing without it: {str(e)}")
                    self.database = None  # Clear database reference if it doesn't exist
            
            # Create schema with qualified name if database exists, otherwise unqualified
            if self.database:
                self.cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {self.database}.{self.schema}")
            else:
                self.cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {self.schema}")
            
            self.cursor.execute(f"USE SCHEMA {self.schema}")
            logger.info(f"Created/using schema {self.schema}")
        except Exception as e:
            logger.error(f"Error creating schema: {str(e)}")
            raise

    def create_dimension_tables(self):
        """Create all dimension tables (no FK constraints)"""
        try:
            # Ensure we're using the correct database and schema
            if self.database:
                self.cursor.execute(f"USE DATABASE {self.database}")
            self.cursor.execute(f"USE SCHEMA {self.schema}")
            
            # CAT_REF - Product category reference
            self.cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.schema}.CAT_REF (
                    cat_cd VARCHAR(50) PRIMARY KEY,
                    cat_nm VARCHAR(255)
                )
            """)
            
            # BK_CAT_REF - Book category reference
            self.cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.schema}.BK_CAT_REF (
                    bk_cat_cd VARCHAR(50) PRIMARY KEY,
                    bk_cat_nm VARCHAR(255)
                )
            """)
            
            # AUTH_REF - Author reference
            self.cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.schema}.AUTH_REF (
                    auth_cd VARCHAR(50) PRIMARY KEY,
                    auth_nm VARCHAR(255)
                )
            """)
            
            # DIR_REF - Director reference
            self.cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.schema}.DIR_REF (
                    dir_cd VARCHAR(50) PRIMARY KEY,
                    dir_nm VARCHAR(255)
                )
            """)
            
            # PERF_REF - Performer/actor reference
            self.cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.schema}.PERF_REF (
                    perf_cd VARCHAR(50) PRIMARY KEY,
                    perf_nm VARCHAR(255)
                )
            """)
            
            # AWARD_REF - Award category reference
            self.cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.schema}.AWARD_REF (
                    awd_cat_cd VARCHAR(50) PRIMARY KEY,
                    awd_nm VARCHAR(255),
                    awd_typ VARCHAR(100)
                )
            """)
            
            logger.info("Created all dimension tables")
        except Exception as e:
            logger.error(f"Error creating dimension tables: {str(e)}")
            raise

    def create_fact_tables(self):
        """Create all fact tables (no FK constraints, just code references)"""
        try:
            # Ensure we're using the correct database and schema
            if self.database:
                self.cursor.execute(f"USE DATABASE {self.database}")
            self.cursor.execute(f"USE SCHEMA {self.schema}")
            
            # INV_MAST - Inventory master (products)
            self.cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.schema}.INV_MAST (
                    inv_id VARCHAR(100) PRIMARY KEY,
                    inv_nm VARCHAR(500),
                    unit_prc FLOAT,
                    cat_cd VARCHAR(50),
                    desc_txt TEXT,
                    lnk_id VARCHAR(100),
                    scrp_dt TIMESTAMP
                )
            """)
            
            # BK_CATALOG - Book catalog
            self.cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.schema}.BK_CATALOG (
                    bk_id VARCHAR(100) PRIMARY KEY,
                    bk_ttl VARCHAR(500),
                    unit_prc FLOAT,
                    auth_cd VARCHAR(50),
                    avail_sts VARCHAR(100),
                    rtg_val INTEGER,
                    desc_txt TEXT,
                    lnk_id VARCHAR(100),
                    scrp_dt TIMESTAMP
                )
            """)
            
            # MEDIA_MAST - Media master (films)
            self.cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.schema}.MEDIA_MAST (
                    media_id VARCHAR(100) PRIMARY KEY,
                    media_ttl VARCHAR(500),
                    yr_val INTEGER,
                    awd_cat_cd VARCHAR(50),
                    dir_cd VARCHAR(50),
                    scrp_dt TIMESTAMP
                )
            """)
            
            logger.info("Created all fact tables")
        except Exception as e:
            logger.error(f"Error creating fact tables: {str(e)}")
            raise

    def create_bridge_tables(self):
        """Create all bridge/junction tables (no FK constraints)"""
        try:
            # Ensure we're using the correct database and schema
            if self.database:
                self.cursor.execute(f"USE DATABASE {self.database}")
            self.cursor.execute(f"USE SCHEMA {self.schema}")
            
            # BK_CAT_XREF - Book category cross-reference
            self.cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.schema}.BK_CAT_XREF (
                    bk_id VARCHAR(100),
                    bk_cat_cd VARCHAR(50),
                    PRIMARY KEY (bk_id, bk_cat_cd)
                )
            """)
            
            # MEDIA_PERF_XREF - Media performer cross-reference
            self.cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.schema}.MEDIA_PERF_XREF (
                    media_id VARCHAR(100),
                    perf_cd VARCHAR(50),
                    role_nm VARCHAR(255),
                    PRIMARY KEY (media_id, perf_cd)
                )
            """)
            
            # MEDIA_AWD_XREF - Media award cross-reference
            self.cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.schema}.MEDIA_AWD_XREF (
                    media_id VARCHAR(100),
                    awd_cat_cd VARCHAR(50),
                    awd_yr INTEGER,
                    PRIMARY KEY (media_id, awd_cat_cd)
                )
            """)
            
            logger.info("Created all bridge tables")
        except Exception as e:
            logger.error(f"Error creating bridge tables: {str(e)}")
            raise

    def create_detail_tables(self):
        """Create all detail tables (no FK constraints)"""
        try:
            # Ensure we're using the correct database and schema
            if self.database:
                self.cursor.execute(f"USE DATABASE {self.database}")
            self.cursor.execute(f"USE SCHEMA {self.schema}")
            
            # INV_VAR - Inventory variant
            self.cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.schema}.INV_VAR (
                    var_id VARCHAR(100) PRIMARY KEY,
                    inv_id VARCHAR(100),
                    sz_cd VARCHAR(50),
                    flv_cd VARCHAR(50),
                    prc_mod FLOAT
                )
            """)
            
            # INV_REV - Inventory review
            self.cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.schema}.INV_REV (
                    rev_id VARCHAR(100) PRIMARY KEY,
                    inv_id VARCHAR(100),
                    rtg_val INTEGER,
                    rev_txt TEXT,
                    rev_nm VARCHAR(255),
                    rev_dt TIMESTAMP
                )
            """)
            
            # INV_SIM - Inventory similarity
            self.cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.schema}.INV_SIM (
                    sim_id VARCHAR(100) PRIMARY KEY,
                    inv_id VARCHAR(100),
                    sim_inv_id VARCHAR(100),
                    sim_score FLOAT
                )
            """)
            
            logger.info("Created all detail tables")
        except Exception as e:
            logger.error(f"Error creating detail tables: {str(e)}")
            raise

    def create_all_tables(self):
        """Create all tables in correct order"""
        self.create_dimension_tables()
        self.create_fact_tables()
        self.create_bridge_tables()
        self.create_detail_tables()

    def load_dataframe(self, df: pd.DataFrame, table_name: str):
        """Load a DataFrame into a Snowflake table"""
        try:
            if df.empty:
                logger.warning(f"DataFrame for {table_name} is empty, skipping")
                return
            
            # Ensure column names are uppercase for Snowflake
            df.columns = [col.upper() for col in df.columns]
            
            # Ensure we're using the correct schema
            if self.database:
                self.cursor.execute(f"USE DATABASE {self.database}")
            self.cursor.execute(f"USE SCHEMA {self.schema}")
            
            # Try write_pandas first, fall back to INSERT if it fails
            try:
                success, nchunks, nrows, _ = write_pandas(
                    conn=self.conn,
                    df=df,
                    table_name=table_name,
                    schema=self.schema,
                    database=self.database,
                    auto_create_table=False,
                    overwrite=False
                )
                
                if success:
                    logger.info(f"Loaded {nrows} rows into {table_name} in {nchunks} chunks")
                else:
                    logger.error(f"Failed to load data into {table_name}")
            except Exception as write_error:
                logger.warning(f"write_pandas failed ({str(write_error)}), using INSERT method")
                # Fallback to INSERT method
                self._load_via_insert(df, table_name)
        except Exception as e:
            logger.error(f"Error loading {table_name}: {str(e)}")
            raise
    
    def _load_via_insert(self, df: pd.DataFrame, table_name: str):
        """Fallback method to load data via INSERT statements"""
        columns = ', '.join(df.columns)
        placeholders = ', '.join(['%s'] * len(df.columns))
        
        # Convert DataFrame to list of tuples, handling datetime objects
        values = []
        for row in df.values:
            row_values = []
            for val in row:
                if pd.isna(val):
                    row_values.append(None)
                elif isinstance(val, pd.Timestamp):
                    row_values.append(val.to_pydatetime())
                else:
                    row_values.append(val)
            values.append(tuple(row_values))
        
        # Insert in batches
        batch_size = 1000
        total_rows = 0
        for i in range(0, len(values), batch_size):
            batch = values[i:i + batch_size]
            insert_sql = f"INSERT INTO {self.schema}.{table_name} ({columns}) VALUES ({placeholders})"
            self.cursor.executemany(insert_sql, batch)
            total_rows += len(batch)
        
        logger.info(f"Loaded {total_rows} rows into {table_name} via INSERT")

    def load_all_data(self, dataframes: Dict[str, pd.DataFrame]):
        """Load all dataframes in correct order: dimensions → facts → bridges → details"""
        logger.info("Loading data into Snowflake tables")
        
        # Load dimension tables first
        dimension_tables = ["CAT_REF", "BK_CAT_REF", "AUTH_REF", "DIR_REF", "PERF_REF", "AWARD_REF"]
        for table in dimension_tables:
            if table in dataframes:
                self.load_dataframe(dataframes[table], table)
        
        # Load fact tables
        fact_tables = ["INV_MAST", "BK_CATALOG", "MEDIA_MAST"]
        for table in fact_tables:
            if table in dataframes:
                self.load_dataframe(dataframes[table], table)
        
        # Load bridge tables
        bridge_tables = ["BK_CAT_XREF", "MEDIA_PERF_XREF", "MEDIA_AWD_XREF"]
        for table in bridge_tables:
            if table in dataframes:
                self.load_dataframe(dataframes[table], table)
        
        # Load detail tables
        detail_tables = ["INV_VAR", "INV_REV", "INV_SIM"]
        for table in detail_tables:
            if table in dataframes:
                self.load_dataframe(dataframes[table], table)
        
        logger.info("All data loaded successfully")

    def create_analytical_views(self):
        """Create analytical views using star schema joins (no FK constraints)"""
        try:
            # Ensure we're using the correct database and schema
            if self.database:
                self.cursor.execute(f"USE DATABASE {self.database}")
            self.cursor.execute(f"USE SCHEMA {self.schema}")
            
            # VW_INV_ANALYTICS - Inventory analytics view
            self.cursor.execute(f"""
                CREATE OR REPLACE VIEW {self.schema}.VW_INV_ANALYTICS AS
                SELECT 
                    i.inv_id,
                    i.inv_nm,
                    c.cat_nm,
                    i.unit_prc,
                    AVG(r.rtg_val) as avg_rtg,
                    COUNT(DISTINCT r.rev_id) as rev_cnt,
                    COUNT(DISTINCT v.var_id) as var_cnt
                FROM {self.schema}.INV_MAST i
                JOIN {self.schema}.CAT_REF c ON i.cat_cd = c.cat_cd
                LEFT JOIN {self.schema}.INV_REV r ON i.inv_id = r.inv_id
                LEFT JOIN {self.schema}.INV_VAR v ON i.inv_id = v.inv_id
                GROUP BY i.inv_id, i.inv_nm, c.cat_nm, i.unit_prc
            """)
            
            # VW_BK_ANALYTICS - Book analytics view
            self.cursor.execute(f"""
                CREATE OR REPLACE VIEW {self.schema}.VW_BK_ANALYTICS AS
                SELECT 
                    b.bk_id,
                    b.bk_ttl,
                    a.auth_nm,
                    b.unit_prc,
                    b.rtg_val,
                    LISTAGG(DISTINCT bc.bk_cat_nm, ', ') WITHIN GROUP (ORDER BY bc.bk_cat_nm) as categories,
                    COUNT(DISTINCT xref.bk_cat_cd) as cat_cnt
                FROM {self.schema}.BK_CATALOG b
                JOIN {self.schema}.AUTH_REF a ON b.auth_cd = a.auth_cd
                LEFT JOIN {self.schema}.BK_CAT_XREF xref ON b.bk_id = xref.bk_id
                LEFT JOIN {self.schema}.BK_CAT_REF bc ON xref.bk_cat_cd = bc.bk_cat_cd
                GROUP BY b.bk_id, b.bk_ttl, a.auth_nm, b.unit_prc, b.rtg_val
            """)
            
            # VW_MEDIA_ANALYTICS - Media analytics view
            self.cursor.execute(f"""
                CREATE OR REPLACE VIEW {self.schema}.VW_MEDIA_ANALYTICS AS
                SELECT 
                    m.media_id,
                    m.media_ttl,
                    m.yr_val,
                    d.dir_nm,
                    aw.awd_nm,
                    COUNT(DISTINCT pxref.perf_cd) as perf_cnt,
                    LISTAGG(DISTINCT p.perf_nm, ', ') WITHIN GROUP (ORDER BY p.perf_nm) as performers
                FROM {self.schema}.MEDIA_MAST m
                JOIN {self.schema}.DIR_REF d ON m.dir_cd = d.dir_cd
                LEFT JOIN {self.schema}.MEDIA_AWD_XREF axref ON m.media_id = axref.media_id
                LEFT JOIN {self.schema}.AWARD_REF aw ON axref.awd_cat_cd = aw.awd_cat_cd
                LEFT JOIN {self.schema}.MEDIA_PERF_XREF pxref ON m.media_id = pxref.media_id
                LEFT JOIN {self.schema}.PERF_REF p ON pxref.perf_cd = p.perf_cd
                GROUP BY m.media_id, m.media_ttl, m.yr_val, d.dir_nm, aw.awd_nm
            """)
            
            logger.info("Created all analytical views")
        except Exception as e:
            logger.error(f"Error creating analytical views: {str(e)}")
            raise

    def verify_data(self) -> Dict[str, int]:
        """Verify data was loaded correctly (row counts only, no FK integrity checks)"""
        results = {}
        
        # Ensure we're using the correct database and schema
        if self.database:
            self.cursor.execute(f"USE DATABASE {self.database}")
        self.cursor.execute(f"USE SCHEMA {self.schema}")
        
        tables = [
            "CAT_REF", "BK_CAT_REF", "AUTH_REF", "DIR_REF", "PERF_REF", "AWARD_REF",
            "INV_MAST", "BK_CATALOG", "MEDIA_MAST",
            "BK_CAT_XREF", "MEDIA_PERF_XREF", "MEDIA_AWD_XREF",
            "INV_VAR", "INV_REV", "INV_SIM"
        ]
        
        try:
            for table in tables:
                self.cursor.execute(f"SELECT COUNT(*) FROM {self.schema}.{table}")
                count = self.cursor.fetchone()[0]
                results[table] = count
                logger.info(f"{table}: {count} rows")
        except Exception as e:
            logger.error(f"Error verifying data: {str(e)}")
            raise
        
        return results

    def create_task(self, task_name: str, query: str, schedule: str = "USING CRON 0 * * * * UTC"):
        """Create a single Snowflake task with the specified query and schedule"""
        try:
            # Ensure we're using the correct database and schema
            if self.database:
                self.cursor.execute(f"USE DATABASE {self.database}")
            self.cursor.execute(f"USE SCHEMA {self.schema}")
            
            # Sanitize task name (remove special characters, ensure valid identifier)
            sanitized_name = task_name.replace(" ", "_").replace("-", "_").upper()
            # Limit length to 255 characters (Snowflake identifier limit)
            if len(sanitized_name) > 255:
                sanitized_name = sanitized_name[:255]
            
            # Create the task
            # Note: Query is embedded directly - these are validated SELECT queries only
            # Warehouse is an identifier, not a string literal
            create_sql = f"""
                CREATE OR REPLACE TASK {self.schema}.{sanitized_name}
                WAREHOUSE = {self.warehouse}
                SCHEDULE = '{schedule}'
                AS
                {query}
            """
            self.cursor.execute(create_sql)
            logger.info(f"Created task: {sanitized_name}")
            
            # Resume the task immediately
            self.resume_task(sanitized_name)
            
            return sanitized_name
        except Exception as e:
            logger.error(f"Error creating task {task_name}: {str(e)}")
            raise

    def resume_task(self, task_name: str):
        """Resume a suspended task"""
        try:
            if self.database:
                self.cursor.execute(f"USE DATABASE {self.database}")
            self.cursor.execute(f"USE SCHEMA {self.schema}")
            
            sanitized_name = task_name.replace(" ", "_").replace("-", "_").upper()
            self.cursor.execute(f"ALTER TASK {self.schema}.{sanitized_name} RESUME")
            logger.info(f"Resumed task: {sanitized_name}")
        except Exception as e:
            logger.error(f"Error resuming task {task_name}: {str(e)}")
            raise

    def suspend_task(self, task_name: str):
        """Suspend a task"""
        try:
            if self.database:
                self.cursor.execute(f"USE DATABASE {self.database}")
            self.cursor.execute(f"USE SCHEMA {self.schema}")
            
            sanitized_name = task_name.replace(" ", "_").replace("-", "_").upper()
            self.cursor.execute(f"ALTER TASK {self.schema}.{sanitized_name} SUSPEND")
            logger.info(f"Suspended task: {sanitized_name}")
        except Exception as e:
            logger.error(f"Error suspending task {task_name}: {str(e)}")
            raise

    def drop_task(self, task_name: str):
        """Drop a task"""
        try:
            if self.database:
                self.cursor.execute(f"USE DATABASE {self.database}")
            self.cursor.execute(f"USE SCHEMA {self.schema}")
            
            sanitized_name = task_name.replace(" ", "_").replace("-", "_").upper()
            self.cursor.execute(f"DROP TASK IF EXISTS {self.schema}.{sanitized_name}")
            logger.info(f"Dropped task: {sanitized_name}")
        except Exception as e:
            logger.error(f"Error dropping task {task_name}: {str(e)}")
            raise

    def list_tasks(self) -> list:
        """List all tasks in the schema"""
        try:
            if self.database:
                self.cursor.execute(f"USE DATABASE {self.database}")
            self.cursor.execute(f"USE SCHEMA {self.schema}")
            
            self.cursor.execute(f"SHOW TASKS IN SCHEMA {self.schema}")
            tasks = self.cursor.fetchall()
            
            # Extract task names from the result
            task_list = []
            if tasks:
                # SHOW TASKS returns columns: created_on, name, database_name, schema_name, owner, comment, warehouse, schedule, state, definition
                # Assuming name is the second column (index 1)
                task_list = [task[1] for task in tasks]
            
            logger.info(f"Found {len(task_list)} tasks in schema {self.schema}")
            return task_list
        except Exception as e:
            logger.error(f"Error listing tasks: {str(e)}")
            raise

    def create_all_tasks(self, queries_json_path: str = "business_queries.json"):
        """Create tasks for all successful queries from the JSON file"""
        import json
        
        try:
            # Load queries from JSON
            with open(queries_json_path, 'r') as f:
                data = json.load(f)
            
            successful_queries = [q for q in data.get("queries", []) if q.get("status") == "success"]
            logger.info(f"Found {len(successful_queries)} successful queries to create tasks for")
            
            created_tasks = []
            failed_tasks = []
            
            for query_data in successful_queries:
                query_id = query_data.get("id")
                category = query_data.get("category", "Unknown")
                query_text = query_data.get("query")
                question = query_data.get("question", "")
                
                if not query_text:
                    logger.warning(f"Skipping query {query_id}: no query text")
                    failed_tasks.append({"id": query_id, "reason": "No query text"})
                    continue
                
                # Create task name: TASK_Q{id}_{category}
                task_name = f"TASK_Q{query_id}_{category}"
                
                try:
                    created_name = self.create_task(task_name, query_text)
                    created_tasks.append({
                        "id": query_id,
                        "name": created_name,
                        "category": category,
                        "question": question
                    })
                except Exception as e:
                    logger.error(f"Failed to create task for query {query_id}: {str(e)}")
                    failed_tasks.append({"id": query_id, "reason": str(e)})
            
            logger.info(f"Successfully created {len(created_tasks)} tasks")
            if failed_tasks:
                logger.warning(f"Failed to create {len(failed_tasks)} tasks")
            
            return {
                "created": created_tasks,
                "failed": failed_tasks,
                "total_created": len(created_tasks),
                "total_failed": len(failed_tasks)
            }
        except Exception as e:
            logger.error(f"Error creating tasks from JSON: {str(e)}")
            raise
