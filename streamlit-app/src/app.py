import streamlit as st

from components.analisis_temporal import show_analisis_temporal
from components.calidad import show_calidad
from components.general import show_general
from components.geografico import show_geografico


def main():
    # Configuración de la página
    st.set_page_config(
        page_title="Sistema de Vacunación MSP",
        page_icon="⚕️",
        layout="wide",
        initial_sidebar_state="collapsed"
    )
    
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
    
    # Información adicional en el sidebar (opcional)
    with st.sidebar:
        st.markdown("### ℹ️ Información")
        st.info("Utiliza las pestañas horizontales superiores para navegar entre las diferentes secciones del sistema.")
        
        # Fecha actual
        from datetime import datetime
        st.markdown(f"**📅 Fecha:** {datetime.now().strftime('%d/%m/%Y')}")


if __name__ == "__main__":
    main()