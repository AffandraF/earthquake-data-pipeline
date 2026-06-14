from urllib.parse import urlparse
import psycopg2
from psycopg2 import sql
import logging
from sqlalchemy import create_engine, text
from src.pandas.schemas import generate_table_ddl

def parse_db_con_str(con_str):
    # Parses SQLAlchemy connection string into standard components.
    parsed = urlparse(con_str)
    username = parsed.username
    password = parsed.password
    host = parsed.hostname
    port = parsed.port or 5432
    db = parsed.path.lstrip('/')
    return host, port, username, password, db

def save_postgres(df, con_str, table_name, key_cols):
    # Saves a Pandas DataFrame into a PostgreSQL table using an Upsert strategy.
    # Writes the dataframe to a staging table, creates the target table structure,
    # performs an INSERT ON CONFLICT DO UPDATE, and drops the staging table.
    
    # Split schema and table names safely
    schema_part, table_part = table_name.split('.')
    staging_table = f"{table_name}_staging"
    staging_schema_part, staging_table_part = staging_table.split('.')
    
    # Create engine for pandas writing
    engine = create_engine(con_str)
    
    # Ensure the target schema exists before writing the staging table
    with engine.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema_part};"))
        
    logging.info(f"Writing Pandas DataFrame to staging table: {staging_table}")
    df.to_sql(
        staging_table_part,
        engine,
        schema=staging_schema_part,
        if_exists="replace",
        index=False
    )
    
    # Connect to the database using psycopg2 to merge the data safely
    host, port, user, password, db = parse_db_con_str(con_str)
    conn = psycopg2.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=db
    )
    
    try:
        with conn.cursor() as cur:
            # Create Schema and Table if not exists
            ddl_sql = generate_table_ddl(table_name)
            logging.info(f"Executing table structure initialization for: {table_name}")
            cur.execute(ddl_sql)
            
            # Build ON CONFLICT DO UPDATE clause
            columns = list(df.columns)
            non_key_cols = [col for col in columns if col not in key_cols]
            
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
        engine.dispose()
