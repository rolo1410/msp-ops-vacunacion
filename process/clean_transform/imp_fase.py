from extract.extraccion_oracle_simple import logger
from process.clean_transform.utils import crear_colulmna_en_tabla, crear_columna_en_tabla_si_no_existe, ejecutar_query

DIC_FASES = [
     {'nombre': 'FASE 0', 'inicio': '2021-01-18', 'fin': '2021-05-23'},
     {'nombre': 'FASE 1', 'inicio': '2021-05-24', 'fin': '2021-06-14'},
     {'nombre': 'FASE 2', 'inicio': '2021-06-15', 'fin': '2021-07-11'},
     {'nombre': 'FASE 3', 'inicio': '2021-07-12', 'fin': '2021-09-05'},
     {'nombre': 'FASE 4', 'inicio': '2021-09-06', 'fin': '2021-09-12'},
     
     
     
     {'nombre': 'Segundo refuerzo', 'inicio': '2022-03-30', 'fin': '2022-03-30'},
     {'nombre': 'Vacunación bivalente 2023', 'inicio': '2023-01-01', 'fin': '2023-12-31'},
     {'nombre': 'Vacunación estacionaria contra COVID-19, 2024', 'inicio': '2024-01-01', 'fin': '2024-12-31'},
     {'nombre': 'Vacunación estacionaria contra COVID 19, 2025', 'inicio': '2025-01-01', 'fin': '2025-12-31'}
]



def imputar_fases_orchester():
    
    query_crear_fase="""
    ALTER TABLE db_vacunacion_covid
    ADD COLUMN IF NOT EXISTS fase_depurada_2 VARCHAR;
    """
    ejecutar_query(
        db_name='resources/data_lake/vacunacion.duckdb',
        query=query_crear_fase
    )
    
    query_crear_audit="""
    ALTER TABLE db_vacunacion_covid
    ADD COLUMN IF NOT EXISTS proceso_auditoria VARCHAR;
    """
    ejecutar_query(
        db_name='resources/data_lake/vacunacion.duckdb',
        query=query_crear_audit
    )
    
    for fase in DIC_FASES:
        query = f"""
        UPDATE db_vacunacion_covid
        SET fase_depurada_2 = '{fase['nombre']}',
            proceso_auditoria = concat(proceso_auditoria, '| P001')
        
        WHERE fecha_aplicacion >= DATE('{fase['inicio']}') AND fecha_aplicacion <= DATE('{fase['fin']}') AND fecha_aplicacion > DATE('2021-01-01')
        """
        ejecutar_query(
            db_name='resources/data_lake/vacunacion.duckdb',
            query=query
        )
        logger.info(f"Imputada fase: {fase['nombre']}")


