# Configuración del Favicon - Sistema MSP

## 🎯 **Objetivo**
Configurar el archivo `faicon.png` como favicon de toda la aplicación Streamlit del Sistema de Vacunación MSP.

## 📁 **Ubicación del Favicon**
```
src/
├── assets/
│   └── images/
│       └── faicon.png  ← Archivo favicon
└── app.py
```

## 🔧 **Implementación Realizada**

### 1. **Función Auxiliar en `utils/helpers.py`**
```python
def get_asset_path(asset_name):
    """Obtiene la ruta a un archivo de asset."""
    current_dir = os.path.dirname(os.path.dirname(__file__))
    asset_path = os.path.join(current_dir, 'assets', 'images', asset_name)
    return asset_path if os.path.exists(asset_path) else None

def get_favicon_path():
    """Obtiene la ruta al favicon de la aplicación."""
    favicon_path = get_asset_path('faicon.png')
    return favicon_path if favicon_path else "⚕️"
```

### 2. **Configuración en `app.py`**
```python
from utils.helpers import get_favicon_path

def main():
    st.set_page_config(
        page_title="Sistema de Vacunación MSP",
        page_icon=get_favicon_path(),  # ← Favicon personalizado
        layout="wide",
        initial_sidebar_state="collapsed"
    )
```

### 3. **Configuración Global `.streamlit/config.toml`**
```toml
[global]
# Configuración global de la aplicación

[theme]
# Tema personalizado basado en MSP
primaryColor = "#FF6B6B"
backgroundColor = "#0E1117"
secondaryBackgroundColor = "#262730"
textColor = "#FAFAFA"
font = "sans serif"
```

## ✅ **Funcionalidades**

### **Favicon Dinámico:**
- ✅ Usa `faicon.png` cuando el archivo existe
- ✅ Fallback a emoji "⚕️" si no se encuentra el archivo
- ✅ Ruta calculada dinámicamente desde la estructura del proyecto

### **Gestión de Assets:**
- ✅ Función `get_asset_path()` para cualquier archivo de asset
- ✅ Función `load_image_as_base64()` para usar imágenes en HTML/CSS
- ✅ Estructura centralizada de assets

### **Configuración Robusta:**
- ✅ Manejo de errores si el archivo no existe
- ✅ Configuración de tema coherente con MSP
- ✅ Configuración global de Streamlit

## 🚀 **Cómo Verificar**

### **1. Ejecutar la Aplicación:**
```bash
cd streamlit-app
streamlit run src/app.py
```

### **2. Verificar en el Navegador:**
- Abrir http://localhost:8501
- Ver el favicon en la pestaña del navegador
- Debería mostrar la imagen `faicon.png`

### **3. Script de Prueba:**
```bash
python test_favicon.py
```

## 🔄 **Comportamiento del Favicon**

| Condición                       | Resultado                   |
| ------------------------------- | --------------------------- |
| `faicon.png` existe y es válido | Usa la imagen personalizada |
| `faicon.png` no existe          | Usa emoji "⚕️" como fallback |
| Error al cargar imagen          | Usa emoji "⚕️" como fallback |

## 📝 **Notas Importantes**

### **Cache del Navegador:**
- El navegador puede cachear el favicon anterior
- Puede ser necesario limpiar caché o usar incógnito
- El favicon puede tardar unos segundos en aparecer

### **Formato del Archivo:**
- Se recomienda PNG para mejor compatibilidad
- Tamaño recomendado: 16x16, 32x32, o 64x64 píxeles
- Peso ligero para carga rápida

### **Uso en Otras Partes:**
```python
from utils.helpers import get_asset_path, load_image_as_base64

# Obtener cualquier asset
logo_path = get_asset_path('logo.png')

# Convertir a base64 para HTML
logo_b64 = load_image_as_base64(logo_path)
```

## 🎉 **Resultado Final**
La aplicación ahora muestra el favicon personalizado `faicon.png` en todas las páginas y pestañas del navegador, proporcionando una identidad visual coherente para el Sistema de Vacunación MSP.