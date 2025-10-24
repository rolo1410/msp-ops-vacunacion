
import logging

import polars as pl

from process.clean_transform.dim_establecimiento import limpiar_columnas_geograficas


def _validar_cedula_profesion(df):
    logging.info("|- Validando cédulas profesionales...")
    df = df.with_columns(
        pl.when(
            (pl.col("tipo_iden") == "CÉDULA DE IDENTIDAD")
            & (~pl.col("num_iden").map_elements(lambda x: len(x) == 10, return_dtype=pl.Boolean))
        )
        .then(pl.lit(True))
        .otherwise(pl.lit(False))
        .alias("cedula_profesional_no_valida"),
    )
    return df

def _cruzar_con_registro_civil(df):
    logging.info("|- Cruzando con registro civil...")
    # Obtener todas las distintas cédulas profesionales en un dataframe
    df_cedulas = df.filter(pl.col("tipo_iden") == "CÉDULA DE IDENTIDAD").select("num_iden").unique()
    df = df.with_columns(
        pl.col("num_iden").filter(pl.col("tipo_iden") == "CÉDULA DE IDENTIDAD").unique().alias("cedulas_profesionales")
    )
    return df

def persona_orchester(df: pl.DataFrame):
    df = _validar_cedula_profesion(df)
    df = _cruzar_con_registro_civil(df)
    return df
