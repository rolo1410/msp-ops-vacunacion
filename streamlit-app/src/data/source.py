import pandas as pd
import duckdb
import os
from dotenv import load_dotenv

# Conexión a DuckDB (ajusta la ruta al archivo si es necesario)
load_dotenv()
DB_PATH = os.getenv("DUCK_DB_PATH")

# Consulta SQL
QUERY_VACUNAS_TEMPORAL_FULL = """
select
	unicodigo,
	'q' uni_lat, 
	'v' uni_long,
	zona,
	'c' circuito,
	'd' distrito,
	'p' provincia,
	'ct' canton,
	'pa' parroquia,
	fecha_aplicacion,
	anio_aplicacion ,
	dia_aplicacion ,
	mes_aplicacion ,
	num_iden,
	nombre_vacuna ,
	dosis_aplicada 
from
	vacunacion.main.vacunacion limit 100542
"""

def get_duck_db_data(query: str) -> pd.DataFrame:
    """Función para ejecutar una consulta SQL en DuckDB y devolver un DataFrame de pandas."""
    con = None
    try:
        con = duckdb.connect(DB_PATH, read_only=True)
        con.execute("SET memory_limit='4GB';")  # Limitar el uso de memoria a 4GB
        result = con.execute(query).df()
        return result
    except Exception as e:
        print(f"Error executing query: {e}")
        return pd.DataFrame()  # Retornar DataFrame vacío en caso de error
    finally:
        if con:
            con.close()
