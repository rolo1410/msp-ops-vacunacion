import polars as pl

from lake.load_lake import load_data
from process.clean_transform.dim_persona import persona_orchester
from process.clean_transform.dim_vacuna import vacuna_orchester
from process.clean_transform.dim_vacunacion import vacunacion_orchester


def process_orchester():
    df = load_data()
    df = persona_orchester(df)
    df = vacuna_orchester(df)
    df = vacunacion_orchester(df)
    return df