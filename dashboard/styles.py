"""
Estilos corporativos compartidos por todas las páginas del dashboard.

Module: dashboard.styles
Purpose: Inyecta CSS personalizado con la identidad visual de Embutidos
    Carchelejo en todas las páginas de Streamlit. Compatible con modo
    claro y modo oscuro. También gestiona la visualización del logo
    corporativo en el sidebar.
Input: dashboard/assets/logo.png (imagen del logo)
Output: Estilos CSS inyectados vía st.markdown + logo en sidebar
Config: Ninguna (los colores están hardcodeados como variables CSS)
Used by: Todas las páginas del dashboard (importan aplicar_estilos
    y mostrar_logo_sidebar)

Identidad visual:
    - Color principal (acento): #F54927 (rojo corporativo)
    - Fondo claro: #FFFFFF / Fondo oscuro: #0E1117
    - Sidebar: #1A1A1A (ambos modos)
    - Tipografía: Segoe UI, Roboto, Helvetica Neue, Arial

Mecanismo de temas:
    Streamlit aplica [data-theme="light"] o [data-theme="dark"] al DOM.
    Los estilos usan custom properties CSS (var(--ec-*)) que cambian
    automáticamente según el tema activo, sin intervención del usuario.
"""
import streamlit as st


def aplicar_estilos():
    """Inyecta los estilos corporativos de Embutidos Carchelejo.

    Define variables CSS para ambos temas (claro y oscuro) y aplica
    estilos a todos los componentes de Streamlit: sidebar, métricas,
    títulos, botones, tabs, expanders, inputs, dataframes, links,
    tooltips, toggles y alertas.

    Debe llamarse al inicio de cada página, antes de cualquier
    componente visual de Streamlit.

    Dependencies:
        - Llamada por: todas las páginas del dashboard
        - Usa: st.markdown con unsafe_allow_html=True
    """
    st.markdown(
        """
        <style>
        /* ============================================================
           VARIABLES DE COLOR POR TEMA
           ============================================================ */

        [data-theme="light"],
        :root {
            --ec-bg-primary: #FFFFFF;
            --ec-bg-secondary: #F7F7F7;
            --ec-bg-sidebar: #1A1A1A;
            --ec-text-primary: #1A1A1A;
            --ec-text-secondary: #333333;
            --ec-text-muted: #666666;
            --ec-text-sidebar: #E0E0E0;
            --ec-text-sidebar-muted: #AAAAAA;
            --ec-border: #E8E8E8;
            --ec-border-light: #E0E0E0;
            --ec-metric-bg: #F7F7F7;
            --ec-accent: #F54927;
            --ec-accent-hover: #D93D1F;
            --ec-accent-soft: rgba(245, 73, 39, 0.08);
        }

        [data-theme="dark"] {
            --ec-bg-primary: #0E1117;
            --ec-bg-secondary: #1E2130;
            --ec-bg-sidebar: #111318;
            --ec-text-primary: #EAEAEA;
            --ec-text-secondary: #C8C8C8;
            --ec-text-muted: #888888;
            --ec-text-sidebar: #E0E0E0;
            --ec-text-sidebar-muted: #999999;
            --ec-border: #2D3040;
            --ec-border-light: #3A3F55;
            --ec-metric-bg: #1E2130;
            --ec-accent: #F54927;
            --ec-accent-hover: #FF6B4A;
            --ec-accent-soft: rgba(245, 73, 39, 0.15);
        }

        /* ============================================================
           ESTILOS GLOBALES
           ============================================================ */

        html, body, [class*="css"] {
            font-family: 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
        }
        .stApp { background-color: var(--ec-bg-primary); }
        section[data-testid="stSidebar"] { background-color: var(--ec-bg-sidebar); }
        section[data-testid="stSidebar"] * { color: var(--ec-text-sidebar) !important; }
        section[data-testid="stSidebar"] .stSelectbox label,
        section[data-testid="stSidebar"] .stTextInput label { color: var(--ec-text-sidebar-muted) !important; }
        div[data-testid="stMetric"] { background-color: var(--ec-metric-bg); border: 1px solid var(--ec-border); border-radius: 8px; padding: 12px 16px; border-left: 4px solid var(--ec-accent); }
        div[data-testid="stMetric"] label { color: var(--ec-text-secondary) !important; font-weight: 600 !important; }
        div[data-testid="stMetric"] div[data-testid="stMetricValue"] { color: var(--ec-text-primary) !important; font-weight: 700 !important; }
        h1 { color: var(--ec-text-primary) !important; border-bottom: 3px solid var(--ec-accent); padding-bottom: 8px; }
        h2, h3 { color: var(--ec-text-secondary) !important; }
        .stButton > button { background-color: var(--ec-accent); color: white !important; border: none; border-radius: 6px; font-weight: 600; transition: background-color 0.2s; }
        .stButton > button:hover { background-color: var(--ec-accent-hover); color: white !important; }
        .stButton > button[kind="primary"] { background-color: var(--ec-accent); }
        .stTabs [data-baseweb="tab-list"] button[aria-selected="true"] { border-bottom-color: var(--ec-accent) !important; color: var(--ec-accent) !important; }
        details[data-testid="stExpander"] summary { color: var(--ec-text-primary) !important; }
        details[data-testid="stExpander"] summary:hover { color: var(--ec-accent) !important; }
        .stSelectbox > div > div, .stTextInput > div > div > input { border-color: var(--ec-border-light); border-radius: 6px; background-color: var(--ec-bg-secondary); color: var(--ec-text-primary); }
        .stSelectbox > div > div:focus-within, .stTextInput > div > div > input:focus { border-color: var(--ec-accent) !important; box-shadow: 0 0 0 1px var(--ec-accent) !important; }
        hr { border-color: var(--ec-border) !important; }
        .stDataFrame { border: 1px solid var(--ec-border); border-radius: 8px; }
        a { color: var(--ec-accent) !important; }
        a:hover { color: var(--ec-accent-hover) !important; }
        .stMarkdown, .stText, p, span, label { color: var(--ec-text-primary); }
        .stCaption, [data-testid="stCaptionContainer"] { color: var(--ec-text-muted) !important; }
        div[data-testid="stMetric"] div[data-testid="stTooltipIcon"] { color: var(--ec-text-muted) !important; }
        .stToggle label span { color: var(--ec-text-primary) !important; }
        details[data-testid="stExpander"] div[data-testid="stExpanderDetails"] { background-color: var(--ec-bg-secondary); border-radius: 0 0 8px 8px; }
        .stAlert { border-radius: 8px; }
        </style>
        """,
        unsafe_allow_html=True,
    )


def mostrar_logo_sidebar():
    """Muestra el logo corporativo en la parte superior del sidebar.

    Lee el archivo dashboard/assets/logo.png, lo codifica en base64 y lo
    inyecta como imagen HTML en el sidebar de Streamlit. El logo se centra
    horizontalmente, se adapta al ancho del sidebar (máximo 80%) y tiene
    una altura máxima de 80px para no ocupar demasiado espacio.

    Si el archivo logo.png no existe, la función no hace nada (no muestra
    error ni espacio vacío).

    Dependencies:
        - Llamada por: todas las páginas del dashboard
        - Fichero: dashboard/assets/logo.png
        - Usa: st.sidebar.markdown con unsafe_allow_html=True
    """
    import base64
    from pathlib import Path

    logo_path = Path(__file__).parent / "assets" / "logo.png"
    if not logo_path.exists():
        return

    with open(logo_path, "rb") as f:
        logo_b64 = base64.b64encode(f.read()).decode()

    st.sidebar.markdown(
        f"""
        <div style="
            display: flex;
            justify-content: center;
            padding: 20px 16px 12px;
            margin-bottom: 8px;
            border-bottom: 1px solid var(--ec-border);
        ">
            <img src="data:image/png;base64,{logo_b64}"
                 style="max-width: 80%; height: auto; max-height: 80px; object-fit: contain;"
                 alt="Embutidos Carchelejo"
            />
        </div>
        """,
        unsafe_allow_html=True,
    )
