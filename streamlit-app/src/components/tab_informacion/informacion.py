import streamlit as st

def show_informacion():

    st.header("Información")
    st.write("Bienvenido al Sistema de Vacunación del Ministerio de Salud Pública. Usa las pestañas para navegar entre los diferentes análisis.")
    
    st.subheader("Objetivos")
    st.markdown(
        """
    - **Objetivo general:** Facilitar el análisis y la visualización de la información de vacunación para apoyar la toma de decisiones del Ministerio de Salud Pública.
    - **Objetivos específicos:**
      - Visualizar coberturas y tendencias por periodo, región y grupos etarios.
      - Proveer herramientas interactivas para explorar resultados y descargar reportes.
    """
    )
    
    st.subheader("COVID-19 en Ecuador")
    st.markdown(
        """
    - Breve contexto: La pandemia de COVID-19 impactó fuertemente al país desde 2020. La vacunación masiva ha sido la principal estrategia para reducir la morbimortalidad y la presión sobre el sistema de salud.
    - Vacunas y esquemas: Se han utilizado diferentes vacunas aprobadas por las autoridades; es importante completar el esquema primario y las dosis de refuerzo recomendadas por el Ministerio de Salud Pública.
    - Cobertura y grupos prioritarios: Programas focalizados para mayores, embarazadas, trabajadores de salud y personas con comorbilidades. Las coberturas varían por provincia y grupo etario.
    - Vigilancia y variantes: La vigilancia genómica y el monitoreo de casos permiten detectar cambios en la transmisión y la aparición de variantes de interés.
    """
    )
    st.markdown(
        """
    Fuentes de datos y vigilancia:
    - Ministerio de Salud Pública (Ecuador): https://www.salud.gob.ec
    - Datos comparativos internacionales: https://ourworldindata.org/coronavirus
    - Repositorios oficiales y reportes epidemiológicos provinciales (consultar los portales del MSP y plataformas de datos abiertos)
    """
    )
    
    st.subheader("Vacunación COVID-19 en Ecuador")
    st.markdown(
        """
    Resumen operativo:
    - Cobertura nacional: la vacunación contra COVID-19 mostró avances importantes desde 2021 con variaciones entre provincias y grupos etarios.
    - Esquema primario: priorizar completar el esquema primario (2 dosis o esquema indicado según vacuna) en población adulta y pediátrica según calendarios del MSP.
    - Dosis de refuerzo: las dosis de refuerzo han sido clave para mantener la protección frente a enfermedad grave; las recomendaciones se actualizan según evidencia y disponibilidad.
    - Grupos prioritarios: adultos mayores, embarazadas, personal de salud y personas con comorbilidades continúan siendo grupos con prioridad para esquemas completos y refuerzos.
    - Desigualdades subnacionales: existen brechas de cobertura entre provincias y zonas urbanas vs rurales que requieren estrategias focalizadas.
    - Vigilancia y seguridad: el monitoreo de eventos adversos y la vigilancia genómica son componentes esenciales para ajustar estrategias de vacunación.
    """
    )

    st.subheader("Interpretación de datos y recomendaciones")
    st.markdown(
        """
    - Interpretar coberturas por edad, provincia y fecha para identificar brechas que justifiquen campañas focalizadas.
    - Combinar datos administrativos (MSP) con encuestas poblacionales y registros de vacunación para validar estimaciones de cobertura.
    - Visualizar tendencias temporales (series de dosis aplicadas por semana/mes) y mapas provinciales para priorizar recursos.
    - Informar campañas de comunicación dirigidas a poblaciones con baja cobertura y facilitar puntos de vacunación accesibles.
    """
    )

    with st.expander("Fuentes oficiales y recursos"):
        st.markdown(
            """
    - Ministerio de Salud Pública (Ecuador): https://www.salud.gob.ec
    - Datos y reportes: portales de datos abiertos del MSP y boletines epidemiológicos provinciales.
    - Comparativos internacionales y contexto: Our World in Data — https://ourworldindata.org/coronavirus
    - Nota para desarrolladores: reemplazar los textos estáticos por métricas y gráficos dinámicos cargando los conjuntos de datos oficiales (CSV/JSON/API). Ejemplos de visualizaciones recomendadas: st.metric para indicadores clave, st.line_chart para tendencias temporales y mapas coropléticos para cobertura por provincia.
    """
        )