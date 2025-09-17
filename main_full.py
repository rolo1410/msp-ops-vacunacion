import argparse
import logging
from dotenv import load_dotenv

from extract.ingest_orchester import ingest_orchester
from lake.init_lake import add_new_elements_to_lake
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
    
    return parser.parse_args()

def main():
    """Función principal del procesamiento"""
    # Configurar logging
    setup_logging()
    
    # Parsear argumentos
    args = parse_arguments()
    
    logging.info(f"Iniciando procesamiento con parámetros: since={args.since}, until={args.until}")
    
    try:
        # Ejecutar ingesta de datos
        logging.info("Iniciando ingesta de datos")
        ingest_orchester(args.since, args.until)
        
        # Ejecutar procesamiento
        logging.info("Iniciando procesamiento de datos")
        df = process_orchester()
        
        # Guardar datos procesados
        logging.info("Guardando datos procesados al lago")
        add_new_elements_to_lake('vacunacion', 'db_vacunacion', ['NUM_IDEN', 'FECHA_APLICACION', 'UNICODIGO'], df)
        
        logging.info("Procesamiento completado exitosamente")
        
    except Exception as e:
        logging.error(f"Error durante el procesamiento: {e}")
        raise

if __name__ == "__main__":
    main()