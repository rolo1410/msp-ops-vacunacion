import streamlit as st

from components.analisis_temporal import show_analisis_temporal
from components.calidad import show_calidad
from components.general import show_general


def apply_custom_css(theme_name):
    """
    Aplica CSS personalizado según el tema seleccionado
    """
    themes = {
        "Oscuro Moderno": {
            "primary": "#FF6B6B",
            "background": "#0E1117",
            "secondary": "#262730",
            "text": "#FAFAFA"
        },
        "Claro Minimalista": {
            "primary": "#1F77B4",
            "background": "#FFFFFF",
            "secondary": "#F8F9FA",
            "text": "#212529"
        },
        "Médico Profesional": {
            "primary": "#2E8B57",
            "background": "#F0F8FF",
            "secondary": "#E6F3FF",
            "text": "#2F4F4F"
        },
        "Salud Pública MSP": {
            "primary": "#0066CC",
            "background": "#FFFFFF",
            "secondary": "#F5F7FA",
            "text": "#1A365D"
        },
        "Oscuro Azul": {
            "primary": "#00D4AA",
            "background": "#1E1E1E",
            "secondary": "#2D2D2D",
            "text": "#FFFFFF"
        }
    }
    
    if theme_name in themes:
        theme = themes[theme_name]
        st.markdown(f"""
        <style>
        .stApp {{
            background-color: {theme['background']};
            color: {theme['text']};
        }}
        .stSidebar {{
            background-color: {theme['secondary']};
        }}
        .stSelectbox > div > div {{
            background-color: {theme['secondary']};
            color: {theme['text']};
        }}
        .stMetric {{
            background-color: {theme['secondary']};
            padding: 10px;
            border-radius: 5px;
            border-left: 4px solid {theme['primary']};
        }}
        .stAlert {{
            background-color: {theme['secondary']};
            color: {theme['text']};
        }}
        </style>
        """, unsafe_allow_html=True)


def main():
    # Configuración de la página
    st.set_page_config(
        page_title="Sistema de Vacunación MSP",
        page_icon="💉",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    # Título principal
    st.title("💉 Sistema de Vacunación MSP")
    
    # Menú lateral
    with st.sidebar:
        st.header("📋 Menú de Navegación")
        
        # Selector de tema
        st.markdown("### 🎨 Configuración de Tema")
        theme_options = [
            "Oscuro Moderno",
            "Claro Minimalista", 
            "Médico Profesional",
            "Salud Pública MSP",
            "Oscuro Azul"
        ]
        
        selected_theme = st.selectbox(
            "Selecciona un tema:",
            theme_options,
            index=0,
            key="theme_selector"
        )
        
        # Aplicar tema seleccionado
        apply_custom_css(selected_theme)
        
        # Información del tema actual
        st.info(f"🎨 Tema activo: **{selected_theme}**")
        
        st.markdown("---")
        
        # Opciones del menú
        menu_options = [
            "🏠 General",
            "✅ Calidad",
            "📈 Análisis Temporal"
        ]
        
        # Selector de página
        selected_page = st.selectbox(
            "Selecciona una sección:",
            menu_options,
            index=0
        )
        
        # Información adicional en el sidebar
        st.markdown("---")
        st.markdown("### ℹ️ Información")
        st.info("Utiliza el menú superior para navegar entre las diferentes secciones del sistema.")
        
        # Fecha actual
        from datetime import datetime
        st.markdown(f"**Fecha:** {datetime.now().strftime('%d/%m/%Y')}")
    
    # Contenido principal basado en la selección
    if selected_page == "🏠 General":
        show_general()
    elif selected_page == "✅ Calidad":
        show_calidad()
    elif selected_page == "📈 Análisis Temporal":
        show_analisis_temporal()


if __name__ == "__main__":
    main()