import logging

import duckdb
import polars as pl


def load_data(db, table) -> pl.DataFrame:
    # Implement the logic to load data into the lake
    logging.info("|- Cargando datos al lago")
    con = duckdb.connect(database=f'./resources/data_lake/{db}.duckdb')
    df = con.execute(f"SELECT * FROM {table}").fetch_df()
    df = pl.from_pandas(df)
    ## convertir los nombres de las columnas a minusculas
    df.columns = [col.lower() for col in df.columns]
    logging.info(" |- Datos cargados al lago")
    return df

def get_identificaciones_data(db, table, column_id) -> pl.DataFrame:
    # Implement the logic to load data into the lake
    logging.info("|- Cargando datos al lago")
    con = duckdb.connect(database=f'./resources/data_lake/{db}.duckdb')
    df = con.execute(f"select distinct(v.{column_id}) from {table} v ").fetch_df()
    ## remover " y ' de las identificaciones :"
    df['num_iden'] = df['num_iden'].str.replace(':','').str.replace('"','').str.replace("'",'')
    df = pl.from_pandas(df)
    logging.info(" |- Datos cargados al lago")
    return df