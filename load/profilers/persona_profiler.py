from datetime import datetime
import logging
import os

import dotenv
import polars as pl

from load.generate_profile import generate_profile_report
dotenv.load_dotenv(override=True)


def __persona_profiler(df: pl.DataFrame):
    try:
        df = df.select(
            'NUM_IDEN',
            'FECHA_APLICACION',
            'UNICODIGO'
        )
        today = datetime.now().strftime("%Y_%m_%d")
        logging.info(f" |- Generando perfil de persona {today}")
        generate_profile_report(df, ['NUM_IDEN', 'FECHA_APLICACION', 'UNICODIGO'],f'./resources/data_out/profiles/{today}', 'Perfil de vacunas aplicadas')
    except Exception as e:
        logging.error(f" |- Error generando perfil de persona {e}")

def __establecimiento_profiler(df: pl.DataFrame):
    try:
        df = df.select(
            'UNICODIGO',
            'PUNTO_VACUNACION'
        )
        today = datetime.now().strftime("%Y_%m_%d")
        logging.info(f" |- Generando perfil de establecimiento {today}")
        generate_profile_report(df, ['UNICODIGO', 'PUNTO_VACUNACION'],f'./resources/data_out/profiles/{today}', 'Perfil de establecimientos')
    except Exception as e:
        logging.error(f" |- Error generando perfil de establecimiento {e}")

def __vacunacion_profiler(df: pl.DataFrame):
    try:
        df = df.select(
            'FECHA_APLICACION',
            'UNICODIGO'
        )
        today = datetime.now().strftime("%Y_%m_%d")
        logging.info(f" |- Generando perfil de vacunacion {today}")
        generate_profile_report(df, ['FECHA_APLICACION', 'UNICODIGO'],f'./resources/data_out/profiles/{today}', 'Perfil de vacunaciones')
    except Exception as e:
        logging.error(f" |- Error generando perfil de vacunacion {e}")
def __vacuna_profiler(df: pl.DataFrame):
    try:
        df = df.select(
            'NOMBRE_VACUNA',
            'LOTE_VACUNA'
        )
        today = datetime.now().strftime("%Y_%m_%d")
        logging.info(f" |- Generando perfil de vacuna {today}")
        generate_profile_report(df, ['NOMBRE_VACUNA', 'LOTE_VACUNA'],f'./resources/data_out/profiles/{today}', 'Perfil de vacunas aplicadas')
    except Exception as e:
        logging.error(f" |- Error generando perfil de vacuna {e}")

def profiler_orchester(df: pl.DataFrame):
    if os.getenv('GENERATE_PERFILER', 'False') == 'True':
        __persona_profiler(df)
        __vacuna_profiler(df)    
        __vacunacion_profiler(df)
        __establecimiento_profiler(df)
    return df