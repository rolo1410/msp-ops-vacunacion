import os
import time

import dotenv
import pandas as pd
import sqlalchemy
from sqlalchemy.engine import URL

dotenv.load_dotenv(override=True)

def fetch_and_save_parquet_oracle(
    user: str,
    password: str,
    host: str,
    port: int,
    service_name: str,
    table_name: str,
    parquet_file: str,
    chunk_size: int = 100000
):

    connection_string = f'oracle+oracledb://{user}:{password}@{host}:{port}/?service_name={service_name}'
    engine: sqlalchemy.Engine = sqlalchemy.create_engine(connection_string, pool_pre_ping=True)

    offset = 0
    dfs = []
    # Get total number of rows (only once, at the start)
    total_query = f"SELECT COUNT(*) FROM {table_name}"
    total_rows = pd.read_sql(total_query, engine).iloc[0, 0]
    total_iters = (total_rows + chunk_size - 1) // chunk_size
    while offset < total_rows:
        iter_num = (offset // chunk_size) + 1
        start_time = time.time()
        query = f"""
            SELECT * FROM (
                SELECT a.*, ROWNUM rnum FROM {table_name} a
                WHERE ROWNUM <= {offset + chunk_size}
            )
            WHERE rnum > {offset}
        """
        df = pd.read_sql(query, engine)
        elapsed = time.time() - start_time
        print(f"Iteración {iter_num} de {total_iters} tiempo de iteración: {elapsed:.2f} segundos")

        if df.empty:
            break
        dfs.append(df)
        offset += chunk_size

    # Concatenate all chunks and save as parquet
    full_df = pd.concat(dfs, ignore_index=True)
    full_df.to_parquet(parquet_file, index=False)

# Example usage:
# fetch_and_save_parquet_oracle(
#     user="your_user",
#     password="your_password",
#     host="your_host",
#     port=1521,
#     service_name="your_service",
#     table_name="db_vacunacion",
#     parquet_file="db_vacunacion.parquet"
# )


# Example usage:
fetch_and_save_parquet_oracle(
    user=os.environ.get("CNN_ORACLE_DB_VACUNACION_USER"),
    password=os.environ.get("CNN_ORACLE_DB_VACUNACION_PASSWORD"),
    host=os.environ.get("CNN_ORACLE_DB_VACUNACION_HOST"),
    port=1521,
    service_name="DB_VACUNACION",
    table_name="HCUE_VACUNACION_DEPURADA.DB_VACUNACION_CONSOLIDADA_DEPURADA_COVID",
    parquet_file="db_covid_19.parquet"
)