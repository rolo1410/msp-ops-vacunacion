from extract.extraccion_oracle_simple import logger
from process.clean_transform.pre_functions import agregar_funciones_utilitarias
from process.clean_transform.utils import ejecutar_query


def _prepare_clean_process():
    logger.info("|-- Preparando tabla de vacunacion_covid, crendo auditoria, logs y tabla base...")
    ## elminiar tabla si existe 
    query = """
    DROP TABLE IF EXISTS db_vacunacion_covid;
    """
    ejecutar_query(
        db_name='resources/data_lake/vacunacion.duckdb',
        query=query
    )
    create_table_from = """
    CREATE TABLE db_vacunacion_covid AS
    SELECT * FROM lk_vacunacion_covid;
   
    """
    ejecutar_query(
        db_name='resources/data_lake/vacunacion.duckdb',
        query=create_table_from
    )
    create_column_audit = """
    ALTER TABLE db_vacunacion_covid
    ADD COLUMN proceso_auditoria VARCHAR;
    """
    ejecutar_query(
        db_name='resources/data_lake/vacunacion.duckdb',
        query=create_column_audit
    )
    
def configuracion_orchester():
    logger.info("|- PREPARACION")
    _prepare_clean_process()
    agregar_funciones_utilitarias()