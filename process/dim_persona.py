
import logging

from polars import DataFrame


def limpiar_columas_texto(df: DataFrame):
    logging.info("|- EST ")
    logging.debug(". |- No implementado")
    return df 


def limpiar_columnas_fecha(df: DataFrame, cols: list):
    logging.info("|- EST ")
    logging.debug(". |- No implementado")
    return df


def limpiar_identificacion(df: DataFrame):
    logging.info("|- EST Estandarizando columnas cédulas")
    logging.debug(". |- No implementadoEstandarizando columnas cédulas")
    return df


def validar_cedulas(df: DataFrame):
    logging.info("|- VAL Estandarizando columnas cédulas")
    logging.debug(". |- No implementadoEstandarizando columnas cédulas")
    return df


def clean_anios_1900(df: DataFrame):
    logging.info("|- VAL Estandarizando columnas cédulas")
    logging.debug(". |- No implementadoEstandarizando columnas cédulas")
    return df


def calcular_edad(df: DataFrame):
    logging.info("|- ENR Agregando edad")
    logging.debug(". |- No implementadoAgregando edad")
    logging.info("|- ENR Desagregando edad en días , mes , año")
    logging.debug(". |- No implementadoDesagregando edad en días , mes , año")
    return df


def persona_orchester(df: DataFrame):
    df = limpiar_columas_texto(df)
    df = limpiar_columnas_fecha(df)
    df = limpiar_identificacion(df)
    return df
