import logging

import polars as pl

from extract.config.sources import DB_VACUNACION, get_oracle_engine

VACUNACION_SCHEMA = {
    "ID_VAC_DEPU": pl.String,
    "FECHA_APLICACION": pl.Date,
    "PUNTO_VACUNACION": pl.String,
    "UNICODIGO": pl.String,
    "TIPO_IDEN": pl.String,
    "NUM_IDEN": pl.String,
    "APELLIDOS": pl.String,
    "NOMBRES": pl.String,
    "NOMBRES_COMPLETOS": pl.String,
    "SEXO": pl.String,
    "FECHA_NACIMIENTO": pl.Date,
    "NACIONALIDAD": pl.String,
    "ETNIA": pl.String,
    "POBLA_VACUNA": pl.String,
    "GRUPO_RIESGO": pl.String,
    "NOMBRE_VACUNA": pl.String,
    "LOTE_VACUNA": pl.String,
    "DOSIS_APLICADA": pl.String,
    "PROFESIONAL_APLICA": pl.String,
    "IDEN_PROFESIONAL_APLICA": pl.String,
    "FASE_VACUNA": pl.String,
    "FASE_VACUNA_DEPURADA": pl.String,
    "GRUPO_RIESGO_DEPURADA": pl.String,
    "SISTEMA": pl.String,
    "REGISTRO_CIVIL": pl.String,
    "ID_VAC_CONS": pl.String,
}

def get_db_vacunacion(since, until, offset=0, chunk_size=100000) -> pl.DataFrame:
    db_vacunacion_engine = get_oracle_engine(DB_VACUNACION)
    query = f"""
            SELECT
                ID_VAC_DEPU,
                FECHA_APLICACION,
                PUNTO_VACUNACION,
                UNICODIGO,
                TIPO_IDEN,
                NUM_IDEN,
                APELLIDOS,
                NOMBRES,
                NOMBRES_COMPLETOS,
                SEXO,
                FECHA_NACIMIENTO,
                NACIONALIDAD,
                ETNIA,
                POBLA_VACUNA,
                GRUPO_RIESGO,
                NOMBRE_VACUNA,
                LOTE_VACUNA,
                DOSIS_APLICADA,
                PROFESIONAL_APLICA,
                IDEN_PROFESIONAL_APLICA,
                FASE_VACUNA,
                FASE_VACUNA_DEPURADA,
                GRUPO_RIESGO_DEPURADA,
                SISTEMA,
                REGISTRO_CIVIL,
                ID_VAC_CONS
            FROM HCUE_VACUNACION_DEPURADA.DB_VACUNACION_CONSOLIDADA_DEPURADA_COVID
            WHERE FECHA_APLICACION BETWEEN TO_DATE('{since}', 'YYYY-MM-DD') AND TO_DATE('{until}', 'YYYY-MM-DD')
            OFFSET {offset} ROWS FETCH NEXT {chunk_size} ROWS ONLY
            """  
    df = pl.read_database(query, connection=db_vacunacion_engine.connect(), infer_schema_length=None)
    return df


def get_count_db_vacunacion(since, until):
    db_vacunacion_engine = get_oracle_engine(DB_VACUNACION)
    query = f"""
            SELECT 
                COUNT(*) AS total_count
            FROM HCUE_VACUNACION_DEPURADA.DB_VACUNACION_CONSOLIDADA_DEPURADA_COVID
            WHERE 
                FECHA_APLICACION BETWEEN TO_DATE('{since}', 'YYYY-MM-DD') 
                AND TO_DATE('{until}', 'YYYY-MM-DD')
            """  # Replace with actual query
    df = pl.read_database(query, connection=db_vacunacion_engine.connect())
    return df['TOTAL_COUNT'][0]


def get_db_vacunaciones(since, until, chunk_size=100000) -> pl.DataFrame:
    logging.info(f"|- Fetching vacunas")
    offset = 0
    all_data = []
    total = get_count_db_vacunacion(since, until)
    for offset in range(0, total, chunk_size):
        logging.info(f" |- Fetching records {offset} to {offset + chunk_size} de un total de {total}")
        chunk = get_db_vacunacion(since, until, offset, chunk_size)
        if chunk.is_empty():
            break
        chunk = chunk.cast(VACUNACION_SCHEMA, strict=False)
        all_data.append(chunk)

    if all_data:
        return pl.concat(all_data)
    else:
        return pl.DataFrame()
