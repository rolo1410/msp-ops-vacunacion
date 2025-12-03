import streamlit as st
from components.calidad import show_calidad
from components.geografico import show_geografico
from components.tab_general.general import show_general
from components.tab_informacion.informacion import show_informacion
from components.tab_temporal.analisis_temporal import show_analisis_temporal

from utils.helpers import get_favicon_path


def main():
    # Configuración de la página con favicon personalizado
    st.set_page_config(
        page_title="Sistema de Vacunación MSP (dev)",
        page_icon=get_favicon_path(),
        layout="wide",
        initial_sidebar_state="collapsed"
    )
    
    # Título principal
    st.title("Sistema de Vacunación MSP (dev)")
    
    # Navegación con tabs horizontales
    tab_informacion, tab1, tab_temporal, tab4, tab_calidad = st.tabs(["Información","General", "Análisis Temporal", "Análisis Geográfico", "Calidad"])

    # Contenido de cada tab
    with tab_informacion:
        show_informacion()
    
    with tab1:
        show_general()
    
    with tab_temporal:
        show_analisis_temporal()
    
    with tab4:
        show_geografico()
    
    with tab_calidad:
        show_calidad()


if __name__ == "__main__":
    main()