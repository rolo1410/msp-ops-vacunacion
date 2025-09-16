import logging

import polars as pl


def limpiar_columnas_geograficas(df: pl.DataFrame, cols: list):
    logging.info("|- EST ")
    logging.debug(" |- Truncando latitud y longitud a 6 decimales")
    df = df.with_columns(
        [pl.col(col).round(6) for col in cols if col in df.columns]
    )  
    return df

def limpiar_columas_texto(df: pl.DataFrame):
    logging.info("|- EST Limpiando columnas de texto")
    logging.debug(" |- Removiendo espacios en blanco y caracteres especiales")
    df = df.with_columns(
        [pl.col(col).str.strip_chars().str.replace_all(r"[^a-zA-Z0-9áéíóúÁÉÍÓÚñÑüÜ\s]", "") for col in df.columns if df[col].dtype == pl.Utf8]
    )
    logging.debug(" |- Convirtiendo a mayúsculas")
    df = df.with_columns(
        [pl.col(col).str.to_uppercase() for col in df.columns if df[col].dtype == pl.Utf8]
    )
    
    return df

def process_establecimiento_orchester(df: pl.DataFrame):
    logging.info("|- Establecimiento Orchester")
    #
    df = limpiar_columnas_geograficas(df, ['LONGPS', 'LATGPS'])
    #
    df = limpiar_columas_texto(df)
    return df