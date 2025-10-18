import streamlit as st

from components.analisis_temporal import show_analisis_temporal
from components.calidad import show_calidad
from components.general import show_general
from components.geografico import show_geografico
from utils.helpers import get_favicon_path


def main():
    # Configuración de la página con favicon personalizado
    st.set_page_config(
        page_title="Sistema de Vacunación MSP",
        page_icon=get_favicon_path(),
        layout="wide",
        initial_sidebar_state="collapsed"
    )
    
    # Ocultar el sidebar completamente con CSS
    st.markdown("""
        <style>
        .css-1d391kg {
            display: none;
        }
        [data-testid="stSidebar"] {
            display: none;
        }
        .css-1lcbmhc {
            display: none;
        }
        .css-17eq0hr {
            display: none;
        }
        /* Ajustar el contenido principal para usar todo el ancho */
        .main .block-container {
            padding-left: 1rem;
            padding-right: 1rem;
            max-width: none;
        }
        </style>
    """, unsafe_allow_html=True)
    
    # Título principal
    st.title("Sistema de Vacunación MSP")
    
    # Navegación con tabs horizontales
    tab1, tab2, tab3, tab4 = st.tabs(["🏠 General", "🔍 Calidad", "📊 Análisis Temporal", "🗺️ Análisis Geográfico"])
    
    # Contenido de cada tab
    with tab1:
        show_general()
    
    with tab2:
        show_calidad()
    
    with tab3:
        show_analisis_temporal()
    
    with tab4:
        show_geografico()


if __name__ == "__main__":
    main()