import datetime
import os

import dotenv
import polars as pl
from load.generate_profile import generate_profile_report
dotenv.load_dotenv()


def __persona_profiler(df: pl.DataFrame):
    df = df.select(
        'NUM_IDEN',
        'FECHA_APLICACION',
        'UNICODIGO'
    )
    today = datetime.now().strftime("%Y_%m_%d")
    generate_profile_report(df, ['NUM_IDEN', 'FECHA_APLICACION', 'UNICODIGO'],f'./resources/data_out/profiles/{today}', 'Perfil de vacunas aplicadas')


def _vacuna_profiler(df: pl.DataFrame):
    df = df.select(
        'NOMBRE_VACUNA',
        'LOTE_VACUNA'
    )
    today = datetime.now().strftime("%Y_%m_%d")
    generate_profile_report(df, ['NOMBRE_VACUNA', 'LOTE_VACUNA'],f'./resources/data_out/profiles/{today}', 'Perfil de vacunas aplicadas')


def profiler_orchester(df: pl.DataFrame):
    if os.getenv('GENERATE_PERFILER') == 'True':
        __persona_profiler(df)
        _vacuna_profiler(df)
    return df