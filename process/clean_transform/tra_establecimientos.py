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


def establecimientos_orchester():
    logger.info("|- TRATAMIENTO DE ESTABLECIMIENTOS")
    _normalizar_unicodigo()