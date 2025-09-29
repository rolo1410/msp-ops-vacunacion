import logging
import os
from datetime import datetime

import polars as pl


def generate_profile_report(df: pl.DataFrame, columns: list[str],path: str, name: str):
    """
    Genera un perfil de datos usando ydata_profiling si está disponible.
    Si no está disponible, genera un reporte básico.
    """
    logging.info("|- ENR Generando perfil de datos")
    
    try:
        from ydata_profiling import ProfileReport
        logging.debug(" |- Usando ydata_profiling para generar perfil completo")
        
        # Seleccionar columnas y convertir a pandas para ydata_profiling
        df_selected = df.select(columns)
        df_pandas = df_selected.to_pandas()
        
        # Crear perfil
        profile = ProfileReport(df_pandas, title=f"{name} Profile Report", explorative=True)
        
        # Guardar archivo
        today = datetime.now().strftime("%Y_%m_%d")
        filename = f"{'_'.join(name.title().split(' '))}_{today}_basic_profile.txt"

        # create path if not exists 
        os.makedirs(path, exist_ok=True)
        
        profile.to_file(os.path.join(path, filename))
        
        logging.info(f" |- Perfil completo generado: {filename}")
        return profile
        
    except ImportError as e:
        logging.warning(f" |- ydata_profiling no disponible: {e}")
        logging.info(" |- Generando reporte básico alternativo")
        return _generate_basic_profile(df, columns, name)
    
    except Exception as e:
        logging.error(f" |- Error generando perfil completo: {e}")
        logging.info(" |- Generando reporte básico alternativo")
        return _generate_basic_profile(df, columns, name)

def _generate_basic_profile(df: pl.DataFrame, columns: list[str], name: str):
    """
    Genera un perfil básico de datos sin dependencias externas
    """
    logging.info(" |- Generando perfil básico")
    
    try:
        df_selected = df.select(columns)
        
        # Información básica
        basic_info = {
            "nombre": name,
            "fecha": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "filas": df_selected.height,
            "columnas": df_selected.width,
            "columnas_nombres": df_selected.columns
        }
        
        # Estadísticas por columna
        stats_per_column = {}
        for col in columns:
            if col in df_selected.columns:
                col_data = df_selected[col]
                stats_per_column[col] = {
                    "tipo": str(col_data.dtype),
                    "valores_nulos": col_data.null_count(),
                    "valores_unicos": col_data.n_unique(),
                    "valores_totales": len(col_data)
                }
                
                # Estadísticas adicionales para columnas numéricas
                if col_data.dtype.is_numeric():
                    try:
                        stats_per_column[col].update({
                            "min": col_data.min(),
                            "max": col_data.max(),
                            "media": col_data.mean(),
                            "mediana": col_data.median()
                        })
                    except Exception as e:
                        logging.debug(f" |- Error calculando estadísticas para {col}: {e}")
        
        # Guardar reporte básico
        today = datetime.now().strftime("%Y_%m_%d")
        filename = f"{'_'.join(name.title().split(' '))}_{today}_basic_profile.txt"
        
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(f"=== PERFIL BÁSICO DE DATOS ===\n")
            f.write(f"Nombre: {basic_info['nombre']}\n")
            f.write(f"Fecha: {basic_info['fecha']}\n")
            f.write(f"Filas: {basic_info['filas']:,}\n")
            f.write(f"Columnas: {basic_info['columnas']}\n\n")
            
            f.write("=== ESTADÍSTICAS POR COLUMNA ===\n")
            for col, stats in stats_per_column.items():
                f.write(f"\nColumna: {col}\n")
                f.write(f"  Tipo: {stats['tipo']}\n")
                f.write(f"  Valores totales: {stats['valores_totales']:,}\n")
                f.write(f"  Valores únicos: {stats['valores_unicos']:,}\n")
                f.write(f"  Valores nulos: {stats['valores_nulos']:,}\n")
                
                if 'min' in stats:
                    f.write(f"  Mínimo: {stats['min']}\n")
                    f.write(f"  Máximo: {stats['max']}\n")
                    f.write(f"  Media: {stats['media']}\n")
                    f.write(f"  Mediana: {stats['mediana']}\n")
        
        logging.info(f" |- Perfil básico generado: {filename}")
        return {"filename": filename, "basic_info": basic_info, "stats": stats_per_column}
    
    except Exception as e:
        logging.error(f" |- Error generando perfil básico: {e}")
        return None