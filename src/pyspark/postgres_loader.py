from urllib.parse import urlparse
import psycopg2
from psycopg2 import sql
import logging

def parse_db_con_str(con_str):
    # Parses SQLAlchemy connection string into JDBC properties.
    parsed = urlparse(con_str)
    username = parsed.username
    password = parsed.password
    host = parsed.hostname
    port = parsed.port or 5432
    db = parsed.path.lstrip('/')
    
    jdbc_url = f"jdbc:postgresql://{host}:{port}/{db}"
    return jdbc_url, username, password

def create_table_ddl(table_name):
    # Returns the DDL statement to initialize schemas and tables.
    # Delegates to the centralized schemas module for single-source-of-truth definitions.
    from src.pyspark.schemas import generate_table_ddl
    return generate_table_ddl(table_name)

def save_postgres(df, con_str, table_name, key_cols):
    # Saves a PySpark DataFrame into a PostgreSQL table using an Upsert strategy.
    # Writes the dataframe to a staging table, creates the target table structure,
    # performs an INSERT ON CONFLICT DO UPDATE, and drops the staging table.
    jdbc_url, user, password = parse_db_con_str(con_str)
    staging_table = f"{table_name}_staging"
    
    # 1. Write PySpark DataFrame to a staging table in the database
    logging.info(f"Writing PySpark DataFrame to staging table: {staging_table}")
    df.write.format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", staging_table) \
        .option("user", user) \
        .option("password", password) \
        .option("driver", "org.postgresql.Driver") \
        .mode("overwrite") \
        .save()
        
    # 2. Connect to the database using psycopg2 to merge the data
    parsed = urlparse(con_str)
    conn = psycopg2.connect(
        host=parsed.hostname,
        port=parsed.port or 5432,
        user=parsed.username,
        password=parsed.password,
        database=parsed.path.lstrip('/')
    )
    
    try:
        with conn.cursor() as cur:
            # Create Schema and Table if not exists
            ddl_sql = create_table_ddl(table_name)
            logging.info(f"Executing table structure initialization for: {table_name}")
            cur.execute(ddl_sql)
            
            # Build ON CONFLICT DO UPDATE clause
            columns = df.columns
            non_key_cols = [col for col in columns if col not in key_cols]
            
            # Split schema and table names safely
            schema_part, table_part = table_name.split('.')
            staging_schema_part, staging_table_part = staging_table.split('.')
            
            # Safely format table names and column names using psycopg2.sql to prevent SQL injection
            target_ident = sql.Identifier(schema_part, table_part)
            staging_ident = sql.Identifier(staging_schema_part, staging_table_part)
            cols_ident = sql.SQL(", ").join(map(sql.Identifier, columns))
            keys_ident = sql.SQL(", ").join(map(sql.Identifier, key_cols))
            
            updates = sql.SQL(", ").join(
                sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(col), sql.Identifier(col))
                for col in non_key_cols
            )
            
            insert_stmt = sql.SQL("""
            INSERT INTO {target} ({cols})
            SELECT {cols} FROM {staging}
            ON CONFLICT ({keys}) DO UPDATE SET
            {updates};
            """).format(
                target=target_ident,
                cols=cols_ident,
                staging=staging_ident,
                keys=keys_ident,
                updates=updates
            )
            
            logging.info(f"Merging data from staging: {staging_table} -> {table_name}")
            cur.execute(insert_stmt)
            
            logging.info(f"Dropping staging table: {staging_table}")
            cur.execute(sql.SQL("DROP TABLE IF EXISTS {staging};").format(staging=staging_ident))
            
        conn.commit()
        logging.info(f"Successfully upserted data to table: {table_name}")
    except Exception as e:
        conn.rollback()
        logging.error(f"Error during Postgres merge operations: {e}")
        raise e
    finally:
        conn.close()
