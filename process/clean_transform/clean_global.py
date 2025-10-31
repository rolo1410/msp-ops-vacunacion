from extract.extraccion_oracle_simple import logger
from process.clean_transform.utils import ejecutar_query


def clean_especial_characters():
    caracteres_especiales = ['$', '%', '&', '/', '(', ')', '=', '?', '¡', '¿', '´', '`', '^', '~', '<', '>', ';', ':', '[', ']', '{', '}', '\\', '|', '*', '+', '-', ',', '.', '!', '@'
                             'ï', '¿', '½'
                             ]
    columns =['num_iden', 'unicodigo', 'sistema']
    for col in columns:
        for char in caracteres_especiales:
            query = f"""
            UPDATE db_vacunacion_covid
            SET {col} = REPLACE({col}, '{char}', ''),
            proceso_auditoria= concat(proceso_auditoria, '| C001, {col}')
            WHERE {col} IS NOT NULL AND {col} LIKE '%{char}%';
            """
            ejecutar_query(
                db_name='resources/data_lake/vacunacion.duckdb',
                query=query
            )
            logger.info(f"---Limpieza de caracterer {char}.")
        logger.info(f"--Limpieza de caracteres especiales completada de la columna {col}.")
    logger.info(f"Limpieza de caracteres especiales completada.")
    

def clean_espacios():
    columns =['num_iden', 'unicodigo', 'sistema']
    for col in columns:
        query = f"""
        UPDATE db_vacunacion_covid
        SET {col} = TRIM({col}),
        proceso_auditoria= concat(proceso_auditoria, '| C001, {col}')
        WHERE {col} IS NOT NULL;
        """
        ejecutar_query(
            db_name='resources/data_lake/vacunacion.duckdb',
            query=query
        )
    logger.info("Limpieza de espacios completada.")



def clean_orchester():
    clean_especial_characters()
    clean_espacios()
    