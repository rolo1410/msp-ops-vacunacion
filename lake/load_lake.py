import logging

import duckdb
import polars as pl


def load_data()-> pl.DataFrame:
    # Implement the logic to load data into the lake
    logging.info("|- Cargando datos al lago")
    con = duckdb.connect(database='./resources/data_lake/vacunacion.duckdb')
    df = con.execute("SELECT * FROM vacunacion").fetch_df()
    df = pl.from_pandas(df)
    logging.info(" |- Datos cargados al lago")
    return df

def get_identificaciones_data()-> pl.DataFrame:
    # Implement the logic to load data into the lake
    logging.info("|- Cargando datos al lago")
    con = duckdb.connect(database='./resources/data_lake/vacunacion.duckdb')
    df = con.execute("select distinct(v.num_iden) from vacunacion v ").fetch_df()
    ## remover " y ' de las identificaciones :"
    df['num_iden'] = df['num_iden'].str.replace(':','').str.replace('"','').str.replace("'",'')
    df = pl.from_pandas(df)
    logging.info(" |- Datos cargados al lago")
    return df