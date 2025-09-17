import logging

import duckdb
import polars as pl


def load_data()-> pl.DataFrame:
    # Implement the logic to load data into the lake
    logging.info("|- Cargando datos al lago")
    con = duckdb.connect(database='./resources/data_lake/vacunacion.duckdb')
    df = con.execute("SELECT * FROM lk_vacunacion").fetch_df()
    df = pl.from_pandas(df)
    logging.info(" |- Datos cargados al lago")
    return df