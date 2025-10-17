"""
Utilidades comunes para la aplicación de vacunación MSP
"""

from datetime import datetime, timedelta
import os

import numpy as np
import pandas as pd
import streamlit as st


def get_asset_path(asset_name):
    """
    Obtiene la ruta a un archivo de asset.
    
    Args:
        asset_name (str): Nombre del archivo de asset
        
    Returns:
        str: Ruta completa al archivo o None si no existe
    """
    # Obtener la ruta base del proyecto
    current_dir = os.path.dirname(os.path.dirname(__file__))  # Subir dos niveles desde utils
    asset_path = os.path.join(current_dir, 'assets', 'images', asset_name)
    
    if os.path.exists(asset_path):
        return asset_path
    else:
        return None


def get_favicon_path():
    """
    Obtiene la ruta al favicon de la aplicación.
    
    Returns:
        str: Ruta al favicon o emoji de fallback
    """
    favicon_path = get_asset_path('faicon.png')
    return favicon_path if favicon_path else "⚕️"


def load_image_as_base64(image_path):
    """
    Carga una imagen y la convierte a base64 para uso en HTML/CSS.
    
    Args:
        image_path (str): Ruta a la imagen
        
    Returns:
        str: Imagen en formato base64 o None si hay error
    """
    try:
        import base64
        with open(image_path, "rb") as img_file:
            return base64.b64encode(img_file.read()).decode()
    except Exception:
        return None


def format_number(number):
    """
    Formatea números con separadores de miles
    """
    return f"{number:,}".replace(",", ".")


def calculate_percentage_change(current, previous):
    """
    Calcula el cambio porcentual entre dos valores
    """
    if previous == 0:
        return 0
    return ((current - previous) / previous) * 100


def generate_sample_data(start_date='2024-01-01', end_date='2024-10-15', freq='D'):
    """
    Genera datos de muestra para el sistema de vacunación
    """
    dates = pd.date_range(start=start_date, end=end_date, freq=freq)
    
    # Simulación de datos de vacunación con tendencia y estacionalidad
    base_value = 2000
    trend = np.linspace(0, 500, len(dates))
    seasonal = 300 * np.sin(2 * np.pi * np.arange(len(dates)) / 365.25)
    weekly_pattern = 200 * np.sin(2 * np.pi * np.arange(len(dates)) / 7)
    noise = np.random.normal(0, 150, len(dates))
    
    values = base_value + trend + seasonal + weekly_pattern + noise
    values = np.maximum(values, 0)  # Asegurar valores no negativos
    
    return pd.DataFrame({
        'fecha': dates,
        'vacunas': values.astype(int)
    })


def get_alert_color(severity):
    """
    Retorna el color apropiado para las alertas según su severidad
    """
    colors = {
        'Alta': '#FF4B4B',
        'Media': '#FFA500',
        'Baja': '#4CAF50'
    }
    return colors.get(severity, '#808080')


def create_metrics_cards(metrics_data):
    """
    Crea tarjetas de métricas usando columnas de Streamlit
    """
    cols = st.columns(len(metrics_data))
    
    for i, (label, value, delta) in enumerate(metrics_data):
        with cols[i]:
            st.metric(label=label, value=value, delta=delta)


def validate_date_range(start_date, end_date):
    """
    Valida que el rango de fechas sea correcto
    """
    if start_date > end_date:
        st.error("La fecha de inicio no puede ser posterior a la fecha de fin")
        return False
    
    if (end_date - start_date).days > 365:
        st.warning("El rango de fechas es muy amplio. Se recomienda un período menor a 1 año")
    
    return True


def export_data_to_csv(data, filename):
    """
    Prepara datos para exportación a CSV
    """
    csv = data.to_csv(index=False)
    return csv


def get_color_scale(values, colorscale='Blues'):
    """
    Genera una escala de colores para visualizaciones
    """
    import plotly.express as px
    return px.colors.sequential.Blues


class DataQualityChecker:
    """
    Clase para verificar la calidad de los datos
    """
    
    @staticmethod
    def check_completeness(df, required_columns):
        """
        Verifica la completitud de las columnas requeridas
        """
        missing_data = {}
        for col in required_columns:
            if col in df.columns:
                missing_count = df[col].isna().sum()
                missing_percentage = (missing_count / len(df)) * 100
                missing_data[col] = {
                    'missing_count': missing_count,
                    'missing_percentage': missing_percentage
                }
        return missing_data
    
    @staticmethod
    def check_duplicates(df, key_columns):
        """
        Verifica duplicados basado en columnas clave
        """
        duplicates = df.duplicated(subset=key_columns).sum()
        duplicate_percentage = (duplicates / len(df)) * 100
        return {
            'duplicate_count': duplicates,
            'duplicate_percentage': duplicate_percentage
        }
    
    @staticmethod
    def check_data_types(df, expected_types):
        """
        Verifica que las columnas tengan los tipos de datos correctos
        """
        type_issues = {}
        for col, expected_type in expected_types.items():
            if col in df.columns:
                actual_type = df[col].dtype
                if str(actual_type) != expected_type:
                    type_issues[col] = {
                        'expected': expected_type,
                        'actual': str(actual_type)
                    }
        return type_issues


def show_loading_spinner(message="Cargando..."):
    """
    Muestra un spinner de carga
    """
    return st.spinner(message)


def create_download_button(data, filename, label="Descargar"):
    """
    Crea un botón de descarga para datos
    """
    if isinstance(data, pd.DataFrame):
        csv = data.to_csv(index=False)
        return st.download_button(
            label=label,
            data=csv,
            file_name=filename,
            mime='text/csv'
        )
    else:
        return st.download_button(
            label=label,
            data=data,
            file_name=filename,
            mime='application/octet-stream'
        )


def get_vaccine_types():
    """
    Retorna la lista de tipos de vacunas disponibles
    """
    return [
        "COVID-19",
        "Influenza",
        "Hepatitis B",
        "MMR (Sarampión, Paperas, Rubéola)",
        "DPT (Difteria, Pertussis, Tétanos)",
        "Polio",
        "BCG",
        "Rotavirus",
        "Neumococo",
        "Varicela"
    ]


def get_regions():
    """
    Retorna la lista de regiones del país
    """
    return [
        "Norte",
        "Sur", 
        "Este",
        "Oeste",
        "Centro",
        "Metropolitana"
    ]


def format_date_spanish(date):
    """
    Formatea fecha en español
    """
    months = {
        1: 'enero', 2: 'febrero', 3: 'marzo', 4: 'abril',
        5: 'mayo', 6: 'junio', 7: 'julio', 8: 'agosto',
        9: 'septiembre', 10: 'octubre', 11: 'noviembre', 12: 'diciembre'
    }
    
    if isinstance(date, str):
        date = datetime.strptime(date, '%Y-%m-%d')
    
    return f"{date.day} de {months[date.month]} de {date.year}"


def calculate_vaccination_rate(vaccinated, population):
    """
    Calcula la tasa de vacunación
    """
    if population == 0:
        return 0
    return (vaccinated / population) * 100


def get_status_icon(status):
    """
    Retorna el ícono apropiado para diferentes estados
    """
    icons = {
        'activo': '✅',
        'inactivo': '❌',
        'pendiente': '⏳',
        'en_proceso': '🔄',
        'completado': '✅',
        'error': '❌',
        'warning': '⚠️',
        'info': 'ℹ️'
    }
    return icons.get(status.lower(), '❓')