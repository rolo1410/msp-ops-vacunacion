import os
import glob
import pandas as pd
import duckdb

def insert_parquets_to_duckdb(input_dir: str, db_path: str, table_name: str):
    con = duckdb.connect(database=db_path)
    
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM parquet_scan('./parquets/db_covid_19.parquet_chunk3.parquet')
    """)
    con.execute(f"""
        DELETE FROM {table_name}
    """)
    for parquet_file in glob.glob(os.path.join(input_dir, "*.parquet")):
        print(f"Inserting {parquet_file} into {table_name}")
        con.execute(f"""
            INSERT INTO {table_name} SELECT * FROM parquet_scan('{parquet_file}')
        """)
    con.close()

# Ejemplo de uso:
insert_parquets_to_duckdb("./parquets", "./parquets/vacunacion.duckdb", "vacunacion")