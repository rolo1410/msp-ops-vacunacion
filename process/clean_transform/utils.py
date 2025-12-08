import logging

import duckdb

from extract.extraccion_oracle_simple import logger


def copia_tabla(db_name, tabla_original, tabla_copia):
    cnn = duckdb.connect(database=db_name, read_only=False)
    try:
        remove_query = f"DROP TABLE IF EXISTS {tabla_copia}"
        cnn.execute(remove_query)   
        query = f"""
        CREATE TABLE {tabla_copia} AS
        SELECT * FROM {tabla_original}
        """
        cnn.execute(query)
        cnn.commit()
    except Exception as e:
        print(f"Error: {e}")
    finally:
        cnn.close()
        
        
def crear_colulmna_en_tabla(db_name, tabla, columna, tipo):
    cnn = duckdb.connect(database=db_name, read_only=False)
    try:
        ## remove column if exists
        query_remove = f"""
        ALTER TABLE {tabla}
        DROP COLUMN IF EXISTS {columna}
        """
        cnn.execute(query_remove)
        cnn.commit()
        
        query = f"""
        ALTER TABLE {tabla}
        ADD COLUMN {columna} {tipo}
        """
        cnn.execute(query)
        cnn.commit()
    except Exception as e:
        print(f"Error: {e}")
    finally:
        cnn.close()
      
def crear_columna_en_tabla_si_no_existe(db_name, tabla, columna, tipo):
    cnn = duckdb.connect(database=db_name, read_only=False)
    try:
        query = f"""
        ALTER TABLE {tabla}
        ADD COLUMN IF NOT EXISTS {columna} {tipo}
        """
        cnn.execute(query)
        cnn.commit()
    except Exception as e:
        print(f"Error -> : {e}")
    finally:
        cnn.close()  
        
def get_from_duckdb(db_name, query):
    cnn = duckdb.connect(database=db_name, read_only=True)
    try:
        result=cnn.execute(query).df()  
        return result
    except Exception as e:
        print(f"Error: {e}")
    finally:
        cnn.close()


def ejecutar_query(db_name, query):
    cnn = duckdb.connect(database=db_name, read_only=False)
    try:
        result = cnn.execute(query).fetchall()
        cnn.commit()
        is_update_or_delete = query.strip().upper().startswith(("UPDATE", "DELETE"))
        if is_update_or_delete:
            logging.info(f"|--- Registros afectados: {result[0][0]} en la {'ACTUALIZACION' if query.strip().upper().startswith('UPDATE') else 'ELIMINACION'}")
        return result
    except Exception as e:
        print(query)
        print(f"Error: {e}")
    finally:
        cnn.close()
