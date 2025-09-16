
import logging

import polars as pl

from process.clean_transform.dim_establecimiento import limpiar_columnas_geograficas


def limpiar_columnas_texto(df: pl.DataFrame, cols: list[str] = []):
    logging.info("|- EST Limpiando columnas de texto")
    for col in cols:
        logging.debug(f"  |- Limpiando columna {col}")
        df = df.with_columns(pl.col(col).str.strip_chars().alias(col))
    return df


def limpiar_columnas_fecha(df: pl.DataFrame, cols: list[str] = []):
    logging.info("|- EST Estandarizando columnas fecha")
    for col in cols:
        logging.info(f"  |- Estandarizando columna {col}")
        
    return df


def esCedulaValida(cedula: str) -> bool:    
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


def limpiar_identificacion(df: pl.DataFrame):
    logging.info("|- LIM Limpiando columnas identificación")
    
    ## Eliminar registros sin cédula
    logging.debug(f"  |- REM Eliminando registros sin cédula o vacíos en el campo NUM_IDEN")
    df = df.filter(pl.col("NUM_IDEN").is_not_null() & (pl.col("NUM_IDEN") != ""))

    ## si el registro es cedula y tiene 10 digitos
    logging.debug(f"  |- EST Completando cedulas que tienen menos de 10 digitos con un 0 a la izquierda")
    df = df.with_columns(pl.when(
        (pl.col("TIPO_IDEN") == "CÉDULA DE IDENTIDAD") & (pl.col("NUM_IDEN").str.len_chars() < 10)
    ).then(
        pl.col("NUM_IDEN").str.zfill(10)
    ).otherwise(
        pl.col("NUM_IDEN")
    ).alias("NUM_IDEN")
)
    
    ## valida si las cédulas cumple con el digito verfificador crear una columna nueva
    logging.debug(f"  |- Identificando cédulas válidas e inválidas")
    df = df.with_columns(
        pl.when(pl.col("TIPO_IDEN") == "CÉDULA DE IDENTIDAD")
            .then(pl.col("NUM_IDEN").apply(esCedulaValida, return_dtype=pl.Boolean))
            .otherwise(None)
            .alias("CEDULA_ES_VALIDA")
    )
    
    return df


def clean_anios_1900(df: pl.DataFrame):
    logging.info("|- VAL Estandarizando columnas cédulas")
    logging.debug(f"  |- No implementadoEstandarizando columnas cédulas")
    return df

def calcular_edad(df: pl.DataFrame):
    logging.info("|- ENR Agregando edad, descompiendo en años, meses y días")
    df['edad_anios'] = df.apply(calcular_edad)
    df['edad_meses'] = df.apply(calcular_edad)
    df['edad_dias'] = df.apply(calcular_edad)
    return df

def calcular_grupo_etario(df: pl.DataFrame):
    # agrega la columna grupo_etario
    logging.info("|- ENR Agregando edad")
    df['grupo_etario'] = df.apply(calcular_edad)    
    logging.debug(". |- No implementadoAgregando edad")
    logging.info("|- ENR Desagregando edad en días , mes , año")
    logging.debug(". |- No implementadoDesagregando edad en días , mes , año")
    return df


def persona_orchester(df: pl.DataFrame):
    df = limpiar_columnas_texto(df, cols=["TIPO_IDEN", "NUM_IDEN", "APELLIDOS", "NOMBRES","NOMBRES_COMPLETOS", "SEXO", "ETNIA", "NACIONALIDAD"])
    df = limpiar_columnas_fecha(df, cols=["FECHA_NACIMIENTO"])
    df = limpiar_identificacion(df)
    df = validar_cedulas(df)
    df = clean_anios_1900(df)
    df = calcular_edad(df)
    df = calcular_grupo_etario(df)
    df = limpiar_identificacion(df)
    return df
