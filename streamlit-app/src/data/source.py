import os

import duckdb
import pandas as pd
from dotenv import load_dotenv

# Conexión a DuckDB (ajusta la ruta al archivo si es necesario)
load_dotenv()
DB_PATH = os.getenv("DUCK_DB_PATH")

def get_duck_db_data(query: str) -> pd.DataFrame:
    """Función para ejecutar una consulta SQL en DuckDB y devolver un DataFrame de pandas."""
    con = None
    try:
        con = duckdb.connect(DB_PATH, read_only=True)
        con.execute("SET memory_limit='4GB';")  # Limitar el uso de memoria a 4GB
        result = con.execute(query).df()
        
        # Aplicar limpieza de tipos de datos
        #result = clean_data_types(result)
        
        return result
    except Exception as e:
        print(f"Error executing query: {e}")
        return pd.DataFrame()  # Retornar DataFrame vacío en caso de error
    finally:
        if con:
            con.close()


def clean_data_types(df: pd.DataFrame) -> pd.DataFrame:
    """
    Limpia y convierte los tipos de datos del DataFrame a los tipos correctos.
    
    Args:
        df: DataFrame con los datos de vacunación
        
    Returns:
        DataFrame con tipos de datos corregidos
    """
    if df.empty:
        return df
    
    # Convertir campos de fecha a enteros
    date_fields = ['anio_aplicacion', 'mes_aplicacion', 'dia_aplicacion']
    for field in date_fields:
        if field in df.columns:
            # Convertir a entero, manejando valores nulos y asegurando que sea entero
            df[field] = pd.to_numeric(df[field], errors='coerce')
            
            # Validaciones específicas para cada campo
            if field == 'anio_aplicacion':
                # Años entre 2020 y 2030 son válidos
                df[field] = df[field].where((df[field] >= 2020) & (df[field] <= 2030), 2024)
            elif field == 'mes_aplicacion':
                # Meses entre 1 y 12
                df[field] = df[field].where((df[field] >= 1) & (df[field] <= 12), 1)
            elif field == 'dia_aplicacion':
                # Días entre 1 y 31
                df[field] = df[field].where((df[field] >= 1) & (df[field] <= 31), 1)
            
            # Convertir a entero después de las validaciones
            df[field] = df[field].fillna(0).astype(int)
    
    # Convertir otros campos numéricos si es necesario
    if 'unicodigo' in df.columns:
        df['unicodigo'] = df['unicodigo'].astype(str)
    
    if 'num_iden' in df.columns:
        df['num_iden'] = df['num_iden'].astype(str)
    
    # Convertir campos de texto a string y limpiar espacios
    text_fields = ['zona', 'circuito', 'distrito', 'provincia', 'canton', 'parroquia', 
                   'nombre_vacuna', 'grupo_etario', 'sexo', 'dosis_aplicada']
    for field in text_fields:
        if field in df.columns:
            df[field] = df[field].astype(str).str.strip()
            # Reemplazar valores 'None' o 'nan' con cadenas vacías
            df[field] = df[field].replace(['None', 'nan', 'NaN'], '')
    
    return df
