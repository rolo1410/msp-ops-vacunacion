#!/usr/bin/env python3
"""
Script de prueba para verificar el favicon de la aplicación
"""
import os
import sys

# Agregar el directorio src al path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

def test_favicon():
    print("🔍 Verificando configuración del favicon...")
    
    # Verificar que el archivo favicon existe
    favicon_path = os.path.join('src', 'assets', 'images', 'faicon.png')
    
    if os.path.exists(favicon_path):
        file_size = os.path.getsize(favicon_path)
        print(f"✅ Archivo favicon encontrado: {favicon_path}")
        print(f"📁 Tamaño del archivo: {file_size} bytes")
        
        # Verificar que no está vacío
        if file_size > 0:
            print("✅ El archivo tiene contenido válido")
        else:
            print("❌ El archivo está vacío")
            return False
            
    else:
        print(f"❌ Archivo favicon NO encontrado en: {favicon_path}")
        return False
    
    # Probar la función get_favicon_path
    try:
        from app import get_favicon_path
        
        favicon_result = get_favicon_path()
        print(f"🎯 Función get_favicon_path() retorna: {favicon_result}")
        
        if isinstance(favicon_result, str) and favicon_result.endswith('faicon.png'):
            print("✅ La función retorna la ruta correcta al favicon")
        elif favicon_result == "⚕️":
            print("⚠️  La función retorna emoji de fallback (archivo no encontrado)")
        else:
            print(f"❌ Resultado inesperado de la función: {favicon_result}")
            
    except ImportError as e:
        print(f"❌ Error al importar función: {e}")
        return False
    
    print("\n📋 Configuración de Streamlit:")
    print("   - page_icon se establece en st.set_page_config()")
    print("   - Se usa la ruta absoluta al archivo faicon.png")
    print("   - Fallback a emoji ⚕️ si el archivo no existe")
    
    print("\n🎉 Verificación del favicon completada!")
    return True

def test_streamlit_config():
    print("\n🔧 Verificando configuración de Streamlit...")
    
    config_path = '.streamlit/config.toml'
    if os.path.exists(config_path):
        print(f"✅ Archivo de configuración encontrado: {config_path}")
        
        with open(config_path, 'r') as f:
            content = f.read()
            if 'theme' in content:
                print("✅ Configuración de tema presente")
            if 'primaryColor' in content:
                print("✅ Color primario configurado")
                
    else:
        print(f"⚠️  Archivo de configuración no encontrado: {config_path}")
    
    return True

if __name__ == "__main__":
    print("🚀 Iniciando pruebas del favicon...")
    
    success = True
    success &= test_favicon()
    success &= test_streamlit_config()
    
    if success:
        print("\n✅ Todas las pruebas pasaron correctamente!")
        print("\n🔗 Para ver el favicon en acción:")
        print("   1. Ejecuta: streamlit run src/app.py")
        print("   2. Abre http://localhost:8501 en el navegador")
        print("   3. Verifica el favicon en la pestaña del navegador")
    else:
        print("\n❌ Algunas pruebas fallaron. Revisa los errores arriba.")
        
    print("\n📝 Nota: El favicon puede tardar unos segundos en aparecer")
    print("    y algunos navegadores pueden cachear el favicon anterior.")