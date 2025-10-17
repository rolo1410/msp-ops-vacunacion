#!/usr/bin/env python3
"""
Verificación rápida del favicon
"""
import os

def check_favicon():
    favicon_path = 'src/assets/images/faicon.png'
    
    if os.path.exists(favicon_path):
        size = os.path.getsize(favicon_path)
        print(f"✅ Favicon encontrado: {favicon_path}")
        print(f"📁 Tamaño: {size:,} bytes")
        return True
    else:
        print(f"❌ Favicon NO encontrado: {favicon_path}")
        return False

def check_app_config():
    app_path = 'src/app.py'
    
    if os.path.exists(app_path):
        with open(app_path, 'r') as f:
            content = f.read()
            if 'get_favicon_path()' in content:
                print("✅ app.py configurado para usar favicon personalizado")
                return True
            else:
                print("❌ app.py NO configurado para favicon")
                return False
    else:
        print("❌ app.py no encontrado")
        return False

if __name__ == "__main__":
    print("🔍 Verificación del Favicon MSP")
    print("=" * 35)
    
    favicon_ok = check_favicon()
    app_ok = check_app_config()
    
    if favicon_ok and app_ok:
        print("\n🎉 ¡Configuración del favicon COMPLETA!")
        print("\n🚀 Para ver el favicon:")
        print("   streamlit run src/app.py")
        print("\n🌐 Luego abrir: http://localhost:8501")
    else:
        print("\n❌ Hay problemas en la configuración")