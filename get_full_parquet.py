import glob
import os
import time

import dotenv
import duckdb
import pandas as pd
import sqlalchemy
from pyspark.sql import SparkSession
from sqlalchemy.engine import URL

dotenv.load_dotenv(override=True)


def merge_parquet_files(input_dir: str, output_file: str):
    spark = SparkSession.builder.getOrCreate()
    parquet_files = glob.glob(os.path.join(input_dir, "*.parquet"))
    print(f"Found {len(parquet_files)} parquet files to merge.")
    if not parquet_files:
        print("No parquet files found in the directory.")
        spark.stop()
        return

    df = spark.read.parquet(*parquet_files)
    df.write.mode("overwrite").parquet(output_file)
    # Eliminar todos los archivos chunk .parquet en el directorio de entrada
    for f in glob.glob(os.path.join(input_dir, "*.parquet")):
        if f != output_file:
            try:
                os.remove(f)
            except Exception as e:
                print(f"No se pudo borrar {f}: {e}")
    spark.stop()

def fetch_and_save_parquet_oracle(
    user: str,
    password: str,
    host: str,
    port: int,
    service_name: str,
    table_name: str,
    parquet_file: str,
<<<<<<< HEAD
    chunk_size: int = 5000000
=======
    chunk_size: int = 100000
>>>>>>> 7e8b8f0 (FEAT: Elimina funciones de caché obsoletas en db_vacunacion.py y optimiza la consulta en get_full_parquet.py aumentando el tamaño del chunk y ajustando la consulta total de filas.)
):

    connection_string = f'oracle+oracledb://{user}:{password}@{host}:{port}/?service_name={service_name}'
    engine: sqlalchemy.Engine = sqlalchemy.create_engine(connection_string, pool_pre_ping=True)

    offset = 0
    # Get total number of rows (only once, at the start)
<<<<<<< HEAD
    total_query = f"SELECT COUNT(*) FROM {table_name} where FECHA_APLICACION <= TO_DATE('2025-01-01', 'YYYY-MM-DD')"
    total_rows = pd.read_sql(total_query, engine).iloc[0, 0]
    total_iters = (total_rows + chunk_size - 1) // chunk_size
    
    # Remove existing parquet file if it exists
    if os.path.exists(parquet_file):
        os.remove(parquet_file)
    
=======
    total_query = f"SELECT COUNT(*) FROM {table_name}"
    total_rows = pd.read_sql(total_query, engine).iloc[0, 0]
    total_iters = (total_rows + chunk_size - 1) // chunk_size
    con = duckdb.connect(f"{parquet_file}.duckdb")
>>>>>>> 7e8b8f0 (FEAT: Elimina funciones de caché obsoletas en db_vacunacion.py y optimiza la consulta en get_full_parquet.py aumentando el tamaño del chunk y ajustando la consulta total de filas.)
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
        con.execute(f"CREATE TABLE IF NOT EXISTS data AS SELECT * FROM df LIMIT 0")
        con.append("data", df)
        print(f"Iteración {iter_num} de {total_iters} tiempo de iteración: {elapsed:.2f} segundos")

        if not df.empty:
            # Agrega columna con el número de iteración
            df['iter_num'] = iter_num

            # Guarda el chunk en un archivo parquet temporal
            temp_parquet = f"./parquets/{parquet_file}_chunk{iter_num}.parquet"
            df.to_parquet(temp_parquet, index=False)
            del df

        offset += chunk_size
<<<<<<< HEAD
    print(f"Proceso completado. Archivo parquet guardado: {parquet_file}")

=======
    con.close()
>>>>>>> 7e8b8f0 (FEAT: Elimina funciones de caché obsoletas en db_vacunacion.py y optimiza la consulta en get_full_parquet.py aumentando el tamaño del chunk y ajustando la consulta total de filas.)

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

merge_parquet_files("./parquets/", "db_covid_19_full.parquet")
