
import logging

import polars as pl

from process.clean_transform.dim_establecimiento import limpiar_columnas_geograficas

# Define qué funciones son públicas
__all__ = [
    'persona_orchester',
]




def _limpiar_columnas_texto(df: pl.DataFrame, cols: list[str] = []):
    logging.info("|- EST Limpiando columnas de texto")
    for col in cols:
        
        logging.debug(f" |- Limpiando columna {col} caracteres especiales")
        df = df.with_columns(pl.col(col).str.strip_chars().alias(col))

        logging.debug(f" |- Limpiando columna {col} mayusculas")
        df = df.with_columns(pl.col(col).str.to_uppercase().alias(col))
    return df


def _limpiar_columnas_fecha(df: pl.DataFrame, cols: list[str] = []):
    logging.info("|- EST Estandarizando columnas fecha")
    for col in cols:
        logging.debug(f" |- Estandarizando columna {col}")
        
    return df


def _es_cedula_valida(cedula: str) -> bool:    
    # Implementación de la validación de cédula
    if not cedula or len(cedula) != 10 or not cedula.isdigit():
        return False

    coeficientes = [2, 1, 2, 1, 2, 1, 2, 1, 2]
    total = 0

    for i in range(9):
        val = int(cedula[i]) * coeficientes[i]
        if val >= 10:
            val -= 9
        total += val

    digito_verificador = 10 - (total % 10) if (total % 10) != 0 else 0

    return digito_verificador == int(cedula[9])


def _limpiar_identificacion(df: pl.DataFrame):
    logging.info("|- LIM Limpiando columnas identificación")
    
    ## Eliminar registros sin cédula
    logging.debug(f" |- REM Eliminando registros sin cédula o vacíos en el campo NUM_IDEN")
    df = df.filter(pl.col("NUM_IDEN").is_not_null() & (pl.col("NUM_IDEN") != ""))

    ## si el registro es cedula y tiene 10 digitos
    logging.debug(f" |- EST Completando cedulas que tienen menos de 10 digitos con un 0 a la izquierda")
    df = df.with_columns(pl.when(
        (pl.col("TIPO_IDEN") == "CÉDULA DE IDENTIDAD") & (pl.col("NUM_IDEN").str.len_chars() < 10)
    ).then(
        pl.col("NUM_IDEN").str.zfill(10)
    ).otherwise(
        pl.col("NUM_IDEN")
    ).alias("NUM_IDEN")
)
    
    ## valida si las cédulas cumple con el digito verfificador crear una columna nueva
    logging.debug(f" |- Identificando cédulas válidas e inválidas")
    df = df.with_columns(
        pl.when(pl.col("TIPO_IDEN") == "CÉDULA DE IDENTIDAD")
            .then(pl.col("NUM_IDEN").map_elements(_es_cedula_valida, return_dtype=pl.Boolean))
            .otherwise(None)
            .alias("CEDULA_ES_VALIDA")
    )
    
    return df


def _calcular_edad(df: pl.DataFrame):
    logging.info("|- ENR Agregando edad, descompiendo en años, meses y días")
    # calcular la edad en años, meses y días usando resta de fechas
    df = df.with_columns(
        (pl.col("FECHA_APLICACION") - pl.col("FECHA_NACIMIENTO")).dt.total_days().alias("EDAD_DIAS")
    )
    
    # Calcular edad en diferentes unidades
    df = df.with_columns([
        (pl.col("EDAD_DIAS") / 365.25).floor().cast(pl.Int32).alias("EDAD_ANIOS"),
        (pl.col("EDAD_DIAS") / 30.44).floor().cast(pl.Int32).alias("EDAD_MESES"), 
        (pl.col("EDAD_DIAS") / 182.62).floor().cast(pl.Int32).alias("EDAD_SEMESTRES")
    ])
    
    return df

def _calcular_grupo_etario(df: pl.DataFrame):
    # agrega la columna grupo_etario
    logging.info("|- ENR Agregando grupo etario")
    logging.debug(" |- No implementado - cálculo de grupo etario")
    logging.info("|- ENR Desagregando edad en días , mes , año")
    logging.debug(" |- No implementado - desagregación de edad")
    return df

def _crear_dataframe_con_moda_fecha(df: pl.DataFrame) -> pl.DataFrame:
    print(df.columns)
    df_moda = (
        df.filter(pl.col("FECHA_APLICACION") != pl.date(1900, 1, 1))
        .group_by("UNICODIGO", "NOMBRE_VACUNA")
        .agg(pl.col("FECHA_APLICACION").mode().first().alias("moda"))
    )
    df_moda.write_csv("df_moda.csv")
    
    df_unido = df.join(df_moda, on=["UNICODIGO", "NOMBRE_VACUNA"], how="left")

    df_final = df_unido.with_columns(
        pl.when(pl.col("FECHA_APLICACION") == pl.date(1900, 1, 1))
        .then(pl.col("moda"))
        .otherwise(pl.col("FECHA_APLICACION"))
        .alias("FECHA_APLICACION_FINAL")
    ).drop("moda")
    
    return df_final

def persona_orchester(df: pl.DataFrame):
    df = _crear_dataframe_con_moda_fecha(df)
    df = _limpiar_columnas_texto(df, cols=["TIPO_IDEN", "NUM_IDEN", "APELLIDOS", "NOMBRES","NOMBRES_COMPLETOS", "SEXO", "ETNIA", "NACIONALIDAD"])
    df = _limpiar_columnas_fecha(df, cols=["FECHA_NACIMIENTO"])
    df = _limpiar_identificacion(df)
    df = _calcular_edad(df)
    df = _calcular_grupo_etario(df)
    return df
