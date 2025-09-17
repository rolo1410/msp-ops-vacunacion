
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


def _calcular_diferencia_fechas(fecha_nacimiento, fecha_aplicacion):
    """
    Calcula la diferencia exacta entre dos fechas en años, meses y días
    """
    from datetime import date
    
    if fecha_nacimiento is None or fecha_aplicacion is None:
        return None, None, None
    
    # Convertir a objetos date si no lo son ya
    if not isinstance(fecha_nacimiento, date):
        return None, None, None
    if not isinstance(fecha_aplicacion, date):
        return None, None, None
    
    # Calcular años
    años = fecha_aplicacion.year - fecha_nacimiento.year
    
    # Calcular meses
    meses = fecha_aplicacion.month - fecha_nacimiento.month
    
    # Calcular días
    dias = fecha_aplicacion.day - fecha_nacimiento.day
    
    # Ajustar si los días son negativos
    if dias < 0:
        meses -= 1
        # Obtener el número de días del mes anterior
        if fecha_aplicacion.month == 1:
            mes_anterior = 12
            año_anterior = fecha_aplicacion.year - 1
        else:
            mes_anterior = fecha_aplicacion.month - 1
            año_anterior = fecha_aplicacion.year
        
        # Días del mes anterior
        if mes_anterior in [1, 3, 5, 7, 8, 10, 12]:
            dias_mes_anterior = 31
        elif mes_anterior in [4, 6, 9, 11]:
            dias_mes_anterior = 30
        else:  # febrero
            if (año_anterior % 4 == 0 and año_anterior % 100 != 0) or (año_anterior % 400 == 0):
                dias_mes_anterior = 29
            else:
                dias_mes_anterior = 28
        
        dias += dias_mes_anterior
    
    # Ajustar si los meses son negativos
    if meses < 0:
        años -= 1
        meses += 12
    
    return años, meses, dias

def _calcular_edad(df: pl.DataFrame):
    logging.info("|- ENR Agregando edad, descomponiendo en años, meses y días")
    
    # Calcular diferencia total en días para referencia
    df = df.with_columns(
        (pl.col("FECHA_APLICACION") - pl.col("FECHA_NACIMIENTO")).dt.total_days().alias("EDAD_TOTAL_DIAS")
    )
    
    # Aplicar la función de cálculo de edad exacta
    logging.debug(" |- Calculando edad exacta en años, meses y días")
    
    df = df.with_columns([
        pl.struct(["FECHA_NACIMIENTO", "FECHA_APLICACION"])
        .map_elements(
            lambda x: _calcular_diferencia_fechas(x["FECHA_NACIMIENTO"], x["FECHA_APLICACION"])[0] if x["FECHA_NACIMIENTO"] is not None and x["FECHA_APLICACION"] is not None else None,
            return_dtype=pl.Int32
        ).alias("EDAD_ANIOS"),
        
        pl.struct(["FECHA_NACIMIENTO", "FECHA_APLICACION"])
        .map_elements(
            lambda x: _calcular_diferencia_fechas(x["FECHA_NACIMIENTO"], x["FECHA_APLICACION"])[1] if x["FECHA_NACIMIENTO"] is not None and x["FECHA_APLICACION"] is not None else None,
            return_dtype=pl.Int32
        ).alias("EDAD_MESES"),
        
        pl.struct(["FECHA_NACIMIENTO", "FECHA_APLICACION"])
        .map_elements(
            lambda x: _calcular_diferencia_fechas(x["FECHA_NACIMIENTO"], x["FECHA_APLICACION"])[2] if x["FECHA_NACIMIENTO"] is not None and x["FECHA_APLICACION"] is not None else None,
            return_dtype=pl.Int32
        ).alias("EDAD_DIAS")
    ])
    logging.debug(" |- Cálculo de edad completado")
    return df


def _calcular_grupo_etario(df: pl.DataFrame):
    """
    Agrega la columna GRUPO_ETARIO basada en la edad en años
    Clasificación estándar epidemiológica por grupos quinquenales
    """
    logging.info("|- ENR Agregando grupo etario")
    
    # Verificar que la columna EDAD_ANIOS existe
    if "EDAD_ANIOS" not in df.columns:
        logging.error(" |- Error: La columna EDAD_ANIOS no existe en el DataFrame")
        return df
    
    logging.debug(" |- Calculando grupos etarios basados en EDAD_ANIOS")
    
    df = df.with_columns(
        pl.when(pl.col("EDAD_ANIOS").is_null())
        .then(pl.lit("NO DEFINIDO"))
        .when(pl.col("EDAD_ANIOS") < 1)
        .then(pl.lit("MENOR DE 1 AÑO"))
        .when(pl.col("EDAD_ANIOS").is_between(1, 4, closed="both"))
        .then(pl.lit("DE 1 A 4 AÑOS"))
        .when(pl.col("EDAD_ANIOS").is_between(5, 9, closed="both"))
        .then(pl.lit("DE 5 A 9 AÑOS"))
        .when(pl.col("EDAD_ANIOS").is_between(10, 14, closed="both"))
        .then(pl.lit("DE 10 A 14 AÑOS"))
        .when(pl.col("EDAD_ANIOS").is_between(15, 19, closed="both"))
        .then(pl.lit("DE 15 A 19 AÑOS"))
        .when(pl.col("EDAD_ANIOS").is_between(20, 64, closed="both"))
        .then(pl.lit("DE 20 A 64 AÑOS"))
        .when(pl.col("EDAD_ANIOS") >= 65)
        .then(pl.lit("DE 80 AÑOS Y MÁS"))
        .otherwise(pl.lit("NO DEFINIDO"))
        .alias("GRUPO_ETARIO")
    )
    

    
    logging.debug(" |- Grupos etarios calculados correctamente")
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

def _homologar_etnia(df: pl.DataFrame):
    logging.info("|- ENR Homologando etnia")
    logging.debug(" |- Homologando etnia")
    etnia_map = pl.read_csv("resources/homologations/per_etnia.csv")
    df = df.join(etnia_map, left_on="ETNIA", right_on="valor_original", suffix="_map")
    df = df.with_columns(pl.col("valor_homologado").alias("ETNIA_HOMOLOGADA"))
    df = df.drop("ETNIA", "valor_homologado") 
    df = df.rename({"ETNIA_HOMOLOGADA": "ETNIA"})
    return df

def persona_orchester(df: pl.DataFrame):
    df = _crear_dataframe_con_moda_fecha(df)
    df = _limpiar_columnas_texto(df, cols=["TIPO_IDEN", "NUM_IDEN", "APELLIDOS", "NOMBRES","NOMBRES_COMPLETOS", "SEXO", "ETNIA", "NACIONALIDAD"])
    df = _limpiar_columnas_fecha(df, cols=["FECHA_NACIMIENTO"])
    df = _limpiar_identificacion(df)
    df = _calcular_edad(df)
    df = _calcular_grupo_etario(df)
    df = _homologar_etnia(df)
    return df
