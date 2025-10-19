import logging
import os
import time

import duckdb
import oracledb
import pandas as pl

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def add_new_elements_to_lake( db:str,
                              table:str,
                              keys_columns:list[str],
                              df:pl.DataFrame):
    logging.info(f"|-Adding new elements to lake: {db}.{table}")
    # Implement the logic to add new elements to the lake
    con = duckdb.connect(f'./resources/data_lake/{db}.duckdb')
    
    # aqui hace el llamado al df
    one_query = f"""CREATE TABLE IF NOT EXISTS {db}.main.{table} AS SELECT * FROM df;
                    CREATE TABLE IF NOT EXISTS {db}.main.tmp_{table} AS SELECT * FROM df;
                    INSERT INTO {db}.main.{table} SELECT * FROM {db}.main.tmp_{table} WHERE NOT EXISTS (SELECT 1 FROM {db}.main.{table} WHERE {' AND '.join([f'{table}.{col} = tmp_{table}.{col}' for col in keys_columns])} );
                    DROP TABLE {db}.main.tmp_{table};"""
    #
    con.execute(one_query)
    con.close()
    
def get_parquet_files():
    """Función principal usando pandas y oracledb directamente"""
    start_time = time.time()
    
    # Asegurar que el directorio de salida existe
    output_dir = "/opt/apps/msp-ops-vacunacion/resources/data_out"
    os.makedirs(output_dir, exist_ok=True)
    
    output_path = os.path.join(output_dir, "DB_VACUNACION_CONSOLIDADA.parquet")
    
    try:
        # Configuración de conexión a Oracle
        logger.info("Conectando a Oracle...")
        
        connection = oracledb.connect(
            user="USR.ROLANDOCASIGNA",
            password="Salud.2025",
            host="scan19c-mspvacuna-prod.msp.gob.ec",
            port=1521,
            service_name="DB_VACUNACION"
        )
        
        logger.info("Conexión exitosa a Oracle")
        
        # Query para obtener los datos
        query = "SELECT * FROM HCUE_AMED.DB_VACUNACION_CONSOLIDADA"
        
        logger.info("Ejecutando query y leyendo datos...")
        
        # Leer datos usando pandas con chunks para manejar datasets grandes
        chunk_size = 5000 # Procesar en chunks de 50k registros
        count=0
        for chunk_df in pl.read_sql(query, connection, chunksize=chunk_size):
            #chunk_df.to_parquet(f"{output_path}_{count}.parquet", compression='snappy', index=False)
            # convertir las clolummas a ministculas 
            chunk_df.columns = [col.lower() for col in chunk_df.columns]
            add_new_elements_to_lake('vacunacion', 'lk_vacunacion_covid', ['num_iden','fecha_aplicacion', 'punto_vacunacion', 'lote_vacuna'], chunk_df)
            logger.info(f"Procesado chunk con {len(chunk_df)} registros")
            count += 1

        logger.info(f"Archivos Parquet guardados exitosamente en: {output_path}")
        
        # Cerrar conexión
        connection.close()
        
        # Calcular tiempo de procesamiento
        end_time = time.time()
        processing_time = end_time - start_time
        processing_time_minutes = processing_time / 60
        
        logger.info(f"Tiempo de procesamiento: {processing_time:.2f} segundos ({processing_time_minutes:.2f} minutos)")
        
        # Mostrar información básica del archivo
        logger.info("Información del archivo generado:")
        logger.info(f"Tamaño del archivo: {os.path.getsize(output_path) / (1024*1024):.2f} MB")
        
        return True
        
    except Exception as e:
        logger.error(f"Error durante la extracción de datos: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return False

def generate_simple_profile(parquet_path):
    """Generar un perfil básico sin ydata-profiling para evitar dependencias"""
    try:
        logger.info("Generando perfil básico de datos...")
        
        df = pl.read_parquet(f"{parquet_path}_*.parquet")
        
        logger.info("=== PERFIL BÁSICO DE DATOS ===")
        logger.info(f"Número de filas: {len(df):,}")
        logger.info(f"Número de columnas: {len(df.columns)}")
        logger.info(f"Tamaño en memoria: {df.memory_usage(deep=True).sum() / (1024*1024):.2f} MB")
        
        logger.info("\n=== INFORMACIÓN POR COLUMNAS ===")
        for col in df.columns:
            logger.info(f"{col}:")
            logger.info(f"  Tipo: {df[col].dtype}")
            logger.info(f"  Valores únicos: {df[col].nunique():,}")
            logger.info(f"  Valores nulos: {df[col].isnull().sum():,}")
            logger.info(f"  % Nulos: {(df[col].isnull().sum() / len(df) * 100):.2f}%")
            
            if df[col].dtype in ['object']:
                logger.info(f"  Valores más frecuentes: {df[col].value_counts().head(3).to_dict()}")
            
            logger.info("")
            
    except Exception as e:
        logger.error(f"Error generando perfil básico: {str(e)}")

if __name__ == "__main__":
    logger.info("=== INICIANDO EXTRACCIÓN DE DATOS ORACLE ===")
    success = get_parquet_files()

    logger.info("=== EXTRACCIÓN COMPLETADA EXITOSAMENTE ===")
    
    # Generar perfil básico
    # parquet_path = "/opt/apps/msp-ops-vacunacion/resources/data_out"
    #generate_simple_profile(parquet_path)