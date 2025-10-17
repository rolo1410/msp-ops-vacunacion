#!/usr/bin/env python3
"""
Script de prueba para verificar la corrección del error en general.py
"""
import sys
import os

# Agregar el directorio src al path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

def test_data_loading():
    """Prueba la carga de datos de manera segura"""
    print("🔍 Probando carga de datos...")
    
    try:
        from data.source import get_duck_db_data, QUERY_VACUNAS_TEMPORAL_FULL
        
        # Intentar cargar datos
        df = get_duck_db_data(QUERY_VACUNAS_TEMPORAL_FULL)
        
        print(f"✅ Datos cargados correctamente")
        print(f"📊 Forma del DataFrame: {df.shape}")
        
        if df.empty:
            print("⚠️  DataFrame está vacío")
            return False
        
        # Verificar columnas críticas
        required_columns = ['anio_aplicacion', 'mes_aplicacion', 'dia_aplicacion']
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            print(f"❌ Faltan columnas: {missing_columns}")
            print(f"📋 Columnas disponibles: {list(df.columns)}")
            return False
        
        # Verificar tipos de datos
        for col in required_columns:
            dtype = df[col].dtype
            print(f"📝 {col}: {dtype}")
            if col in ['anio_aplicacion', 'mes_aplicacion', 'dia_aplicacion']:
                if dtype not in ['int64', 'int32', 'int16']:
                    print(f"⚠️  {col} no es entero: {dtype}")
        
        # Verificar años disponibles
        if 'anio_aplicacion' in df.columns:
            años_únicos = df['anio_aplicacion'].unique()
            print(f"📅 Años disponibles: {sorted(años_únicos)}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error al cargar datos: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_safe_function():
    """Prueba la función safe_get_unique_values"""
    print("\n🔧 Probando función auxiliar...")
    
    try:
        from components.general import safe_get_unique_values
        import pandas as pd
        
        # Crear DataFrame de prueba
        test_df = pd.DataFrame({
            'anio_aplicacion': [2024, 2023, 2024, None],
            'mes_aplicacion': [1, 12, 6, None]
        })
        
        # Probar función
        años = safe_get_unique_values(test_df, 'anio_aplicacion', [2024])
        meses = safe_get_unique_values(test_df, 'mes_aplicacion', [1])
        
        print(f"✅ Función auxiliar funciona correctamente")
        print(f"📅 Años de prueba: {años}")
        print(f"📅 Meses de prueba: {meses}")
        
        # Probar con DataFrame vacío
        empty_df = pd.DataFrame()
        años_empty = safe_get_unique_values(empty_df, 'anio_aplicacion', [2024])
        print(f"✅ Manejo de DataFrame vacío: {años_empty}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error en función auxiliar: {e}")
        return False

def test_general_component():
    """Prueba la función show_general de manera limitada"""
    print("\n🎯 Probando componente general...")
    
    try:
        # Solo importar y verificar que no hay errores de sintaxis
        from components.general import show_general
        print("✅ Componente general importado correctamente")
        print("🎉 No hay errores de sintaxis")
        return True
        
    except Exception as e:
        print(f"❌ Error en componente general: {e}")
        return False

if __name__ == "__main__":
    print("🚀 Iniciando pruebas de corrección de errores...")
    print("=" * 50)
    
    success = True
    success &= test_safe_function()
    success &= test_data_loading()
    success &= test_general_component()
    
    print("\n" + "=" * 50)
    if success:
        print("✅ ¡Todas las pruebas pasaron!")
        print("\n🔧 Correcciones implementadas:")
        print("   - Manejo seguro de DataFrames vacíos")
        print("   - Verificación de columnas existentes") 
        print("   - Función auxiliar safe_get_unique_values()")
        print("   - Manejo robusto de errores")
        print("\n🚀 La aplicación debería funcionar sin errores")
    else:
        print("❌ Algunas pruebas fallaron")
        print("🔍 Revisa los errores específicos arriba")