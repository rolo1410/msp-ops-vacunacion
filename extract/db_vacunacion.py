import polars as pl

from extract.config.sources import DB_VACUNACION, get_oracle_engine


def get_db_vacunacion(since, until, offset=0, chunk_size=100000):
    db_vacunacion_engine = get_oracle_engine(DB_VACUNACION)
    query = """
            SELECT 
                ID_VAC_DEPU,
                FECHA_APLICACION,
                PUNTO_VACUNACION,
                UNICODIGO,
                APELLIDOS,
                TIPO_IDEN,
                NUM_IDEN,
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
            WHERE FECHA_APLICACION BETWEEN TO_DATE(:since, 'YYYY-MM-DD') AND TO_DATE(:until, 'YYYY-MM-DD')
            OFFSET :offset ROWS FETCH NEXT :chunk_size ROWS ONLY
            """  # Replace with actual query
    df = pl.read_sql(query, db_vacunacion_engine)
    return df


def get_count_db_vacunacion(since, until):
    db_vacunacion_engine = get_oracle_engine(DB_VACUNACION)
    query = """
            SELECT 
                COUNT(*) AS total_count
            FROM HCUE_VACUNACION_DEPURADA.DB_VACUNACION_CONSOLIDADA_DEPURADA_COVID
            WHERE 
                FECHA_APLICACION BETWEEN TO_DATE(:since, 'YYYY-MM-DD') 
                AND TO_DATE(:until, 'YYYY-MM-DD')
            """  # Replace with actual query
    df = pl.read_sql(query,
                     db_vacunacion_engine,
                     params={"since": since, "until": until})
    
    return df['total_count'][0]


def get_db_vacunaciones(since, until):
    offset = 0
    chunk_size = 100000
    all_data = []

    while True:
        chunk = get_db_vacunacion(since, until, offset, chunk_size)
        if chunk.is_empty():
            break
        all_data.append(chunk)
        offset += chunk_size

    if all_data:
        return pl.concat(all_data)
    else:
        return pl.DataFrame()