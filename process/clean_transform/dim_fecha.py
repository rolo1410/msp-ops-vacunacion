import logging

import polars as pl


def generar_dim_fecha(df: pl.DataFrame):
    logging.info("|- DIM Fecha")
    ## generar fechas desde 2020-01-01 hasta 2025-12-31
    df = df.with_columns(
        pl.date_range(
            start="2020-01-01",
            end="2025-12-31",
            closed="both",
            name="FECHA"
        )
    )   
    return df