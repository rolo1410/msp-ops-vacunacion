from extract.extraccion_oracle_simple import logger
from process.clean_transform.utils import crear_columna_en_tabla_si_no_existe, ejecutar_query


def add_diff_dias_fecha_aplicacion():
    logger.info("Agregando columna diff_dias_fecha_aplicacion")
    '''
    Agrega la columna de diferencia de días desde la fecha de aplicación a la fecha actual
    '''
    crear_columna_en_tabla_si_no_existe(
        db_name='resources/data_lake/vacunacion.duckdb',
        tabla='db_vacunacion_covid',
        columna='diff_dias_fecha_aplicacion',
        tipo='INTEGER'
    )
    
    query = f"""
                UPDATE db_vacunacion_covid
                SET diff_dias_fecha_aplicacion = T2.diferencia_dias
                FROM (
                WITH FechasUnicasCalculadas AS (
                SELECT DISTINCT
                num_iden,
                fecha_aplicacion,
                DATE_DIFF('day', 
                    LAG(fecha_aplicacion, 1) OVER (
                        PARTITION BY num_iden
                        ORDER BY fecha_aplicacion
                    ), 
                    fecha_aplicacion
                ) AS diferencia_dias
                FROM
                db_vacunacion_covid
                )
                SELECT * FROM FechasUnicasCalculadas
                ) AS T2
                WHERE db_vacunacion_covid.num_iden = T2.num_iden
                AND db_vacunacion_covid.fecha_aplicacion = T2.fecha_aplicacion;
    """
    ejecutar_query(
        db_name='resources/data_lake/vacunacion.duckdb',
        query=query
    )   
    logger.info("Columna diff_dias_fecha_aplicacion agregada y calculada.")

def add_semana_epidemiologica():
    '''
    Agrega la columna de semana epidemiológica a la tabla de vacunación
    '''
    logger.info("Agregando columna semana_epidemiologica")
    pass


def add_data_orchester():
    '''
    Agrega datos adicionales a la tabla de vacunación
    '''
    add_semana_epidemiologica()
    add_diff_dias_fecha_aplicacion()