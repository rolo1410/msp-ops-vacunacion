import argparse
import logging
from datetime import datetime

from dotenv import load_dotenv

from extract.ingest_orchester import ingest_orchester
from lake.init_lake import add_new_elements_to_lake
from load.profilers.persona_profiler import profiler_orchester
from process.clean_transform_orchester import process_orchester

load_dotenv(override=True) 

def setup_logging():
    """Configurar el sistema de logging"""
    # Logging en consola y archivo
    logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s")
    
    # Crear un handler para archivo
    file_handler = logging.FileHandler('app.log')
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    
    # Agregar el handler de archivo al logger root
    logging.getLogger().addHandler(file_handler)

def parse_arguments():
    """Parsear argumentos de línea de comandos"""
    parser = argparse.ArgumentParser(
        description="Procesamiento completo de datos de vacunación",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    
    parser.add_argument(
        '--since', 
        type=str, 
        default='1800-01-01',
        help='Fecha de inicio para la extracción de datos (formato: YYYY-MM-DD)'
    )
    
    parser.add_argument(
        '--until', 
        type=str, 
        default='2022-01-01',
        help='Fecha de fin para la extracción de datos (formato: YYYY-MM-DD)'
    )
    
    parser.add_argument(
        '--chunk-size',
        type=int,
        default=500000,
        help='Tamaño de chunk para procesamiento paralelo (default: 500000)'
    )
    
    parser.add_argument(
        '--max-workers',
        type=int,
        default=4,
        help='Número máximo de workers para procesamiento paralelo (default: 4)'
    )
    
    parser.add_argument(
        '--no-cache',
        action='store_true',
        help='Deshabilitar cache y forzar consulta a base de datos'
    )
    
    return parser.parse_args()

def main():
    """Función principal del procesamiento"""
    # Configurar logging
    setup_logging()
    
    # Parsear argumentos
    args = parse_arguments()
    
    logging.info(f"Iniciando procesamiento con parámetros:")
    logging.info(f"  - since: {args.since}")
    logging.info(f"  - until: {args.until}")
    logging.info(f"  - chunk_size: {args.chunk_size:,}")
    logging.info(f"  - max_workers: {args.max_workers}")
    logging.info(f"  - cache habilitado: {not args.no_cache}")
    
    try:
        # Ejecutar ingesta de datos con parámetros optimizados
        logging.info("Iniciando ingesta de datos")
        ingest_orchester(args.since, args.until, args.chunk_size)
        
        # Ejecutar procesamiento
        logging.info("Iniciando procesamiento de datos")
        df = process_orchester()
        
        # Guardar datos procesados
        logging.info("Guardando datos procesados al lago")
        add_new_elements_to_lake('vacunacion', 'db_vacunacion', ['NUM_IDEN', 'FECHA_APLICACION', 'UNICODIGO'], df)
        
        # Guardar datos procesados
        logging.info("Generando perfiles de datos")
        profiler_orchester(df)  # pyright: ignore[reportUndefinedVariable]
        logging.info("Procesamiento completado exitosamente")
        
    except Exception as e:
        logging.error(f"Error durante el procesamiento: {e}")
        raise

if __name__ == "__main__":
    main()