import logging
import os
from datetime import datetime

import dotenv
import polars as pl

from load.generate_profile import generate_profile_report

dotenv.load_dotenv(override=True)

def __generic_profiler(df: pl.DataFrame, schema:str,  columns: list[str], name: str):
    try:
        df = df.select(columns)
        today = datetime.now().strftime("%Y_%m_%d")
        logging.info(f" |- Generando perfil de {name} {today}")
        generate_profile_report(df, columns,f'./resources/data_out/profiles/{schema}/{today}', f'Perfil de {name}')
    except Exception as e:
        logging.error(f" |- Error generando perfil de {name} {e}")


def profiler_orchester(df: pl.DataFrame):
    if os.getenv('GENERATE_PERFILER', 'False') == 'True':
        __generic_profiler(df,'vacuna', ['NOMBRE_VACUNA', 'LOTE_VACUNA'], 'vacuna')
        __generic_profiler(df,'vacunacion', ['FECHA_APLICACION', 'UNICODIGO'], 'vacunacion')
        __generic_profiler(df,'establecimiento', ['UNICODIGO', 'PUNTO_VACUNACION'], 'establecimiento')
        __generic_profiler(df,'persona', ['NUM_IDEN', 'FECHA_APLICACION', 'UNICODIGO'], 'persona')
    return df