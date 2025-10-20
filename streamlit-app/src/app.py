import streamlit as st

from components.geografico import show_geografico
from components.tab_general.general import show_general
from utils.helpers import get_favicon_path


def main():
    # Configuración de la página con favicon personalizado
    st.set_page_config(
        page_title="Sistema de Vacunación MSP",
        page_icon=get_favicon_path(),
        layout="wide",
        initial_sidebar_state="collapsed"
    )
    
    # Título principal
    st.title("Sistema de Vacunación MSP")
    
    # Navegación con tabs horizontales
    tab1,  tab4 = st.tabs(["General",  "Análisis Geográfico"])
    
    # Contenido de cada tab
    with tab1:
        show_general()
    
    #with tab3:
    #    show_analisis_temporal()
    
    with tab4:
        show_geografico()


if __name__ == "__main__":
    main()