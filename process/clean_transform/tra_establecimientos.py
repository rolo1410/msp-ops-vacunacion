import logging

from process.clean_transform.utils import ejecutar_query

logger = logging.getLogger(__name__)


def _normalizar_unicodigo():
    """
    Normaliza el campo unicodigo para que siempre tenga 6 caracteres,
    agregando ceros a la izquierda cuando sea necesario.
    """
    logger.info("|-- Normalizando unicodigo a 6 caracteres con ceros a la izquierda")
    query = """
    UPDATE db_vacunacion_covid
    SET unicodigo = LPAD(CAST(unicodigo AS VARCHAR), 6, '0'),
        proceso_auditoria = CONCAT(proceso_auditoria, '| EST_001')
    WHERE LENGTH(CAST(unicodigo AS VARCHAR)) < 6
    """
    ejecutar_query(
        db_name='resources/data_lake/vacunacion.duckdb',
        query=query
    )

def _eliminar_espacios_unicodigo():
    """
    Elimina espacios en blanco en el campo unicodigo.
    """
    logger.info("|-- Eliminando espacios en blanco en unicodigo")
    query = """
    UPDATE db_vacunacion_covid
    SET unicodigo = TRIM(CAST(unicodigo AS VARCHAR)),
        proceso_auditoria = CONCAT(proceso_auditoria, '| EST_002')
    WHERE unicodigo IS NOT NULL AND (unicodigo LIKE ' %' OR unicodigo LIKE '% ')
    """
    ejecutar_query(
        db_name='resources/data_lake/vacunacion.duckdb',
        query=query
    )
    
def _eliminar_espacios_uni_nombre():
    """
    Elimina espacios en blanco en el campo nombre_establecimiento.
    """
    logger.info("|-- Eliminando espacios en blanco en nombre_establecimiento")
    query = """
    UPDATE db_vacunacion_covid
    SET uni_nombre = TRIM(CAST(uni_nombre AS VARCHAR)),
        proceso_auditoria = CONCAT(proceso_auditoria, '| EST_003')
    WHERE uni_nombre IS NOT NULL AND (uni_nombre LIKE ' %' OR uni_nombre LIKE '% ')
    """
    ejecutar_query(
        db_name='resources/data_lake/vacunacion.duckdb',
        query=query
    )
    
def _eliminar_caracteres_especiales_nombre_establecimiento():
    """
    Elimina caracteres especiales en el campo nombre_establecimiento.
    """
    logger.info("|-- Eliminando caracteres especiales en nombre_establecimiento")
    query = """
    UPDATE db_vacunacion_covid
    SET uni_nombre = REGEXP_REPLACE(CAST(uni_nombre AS VARCHAR), '[^a-zA-Z0-9áéíóúÁÉÍÓÚñÑüÜ ]', ''),
        proceso_auditoria = CONCAT(proceso_auditoria, '| EST_004')
    WHERE uni_nombre IS NOT NULL AND REGEXP_LIKE(CAST(uni_nombre AS VARCHAR), '[^a-zA-Z0-9áéíóúÁÉÍÓÚñÑüÜ ]')
    """
    ejecutar_query(
        db_name='resources/data_lake/vacunacion.duckdb',
        query=query
    )

def establecimientos_orchester():
    logger.info("|- TRATAMIENTO DE ESTABLECIMIENTOS")
    _normalizar_unicodigo()
    _eliminar_espacios_unicodigo()
    _eliminar_espacios_uni_nombre()
    _eliminar_caracteres_especiales_nombre_establecimiento()
    