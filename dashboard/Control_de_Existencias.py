"""
Panel General — Página principal del dashboard.

Module: dashboard.Control_de_Existencias
Purpose: Vista ejecutiva para dirección. Muestra el estado global del stock
    con KPIs de artículos afectados, tabla filtrable de artículos con su
    estado de alerta, gráfico de cobertura por artículo, distribución de
    alertas por tipo y lista de pedidos en tránsito.
Input: data/raw/EXISTENCIAS_MINIMO.xlsx (lectura en tiempo real vía ETL)
Output: Interfaz web Streamlit (página principal del dashboard)
Config: config/settings.yaml (todas las secciones)
Used by: Punto de entrada del dashboard (streamlit run Control_de_Existencias.py)

Componentes visuales:
    1. KPIs: Artículos Críticos, Artículos en Riesgo, Stock Correcto, Cobertura Media
       → Cuentan artículos únicos, no alertas individuales (a diferencia de
         pages/1_Alertas_Activas.py que cuenta alertas totales)
    2. Tabla de artículos: filtrable por estado, proveedor y búsqueda libre
       → Normaliza artículos con .str.strip() para evitar desajustes de espacios
    3. Gráfico cobertura: barras coloreadas (rojo < 1 sem, ámbar 1-3, verde > 3)
    4. Distribución alertas: barras horizontales por tipo (A1–A10)
    5. Pedidos en tránsito: tabla con artículo, cantidad, proveedor

Nota sobre cache: @st.cache_data(ttl=300) cachea los datos durante 5 minutos.
    Para forzar recarga, usar el botón de Streamlit o limpiar cache desde Admin.
"""
import sys
from pathlib import Path

# Asegurar imports desde la raíz del proyecto
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go

from src.utils.config_loader import load_config, is_page_enabled
from src.etl.etl_existencias import extraer_datos_erp, transformar_datos
from src.alerts.rules_existencias import evaluar_alertas
from styles import aplicar_estilos, mostrar_logo_sidebar
aplicar_estilos()
mostrar_logo_sidebar()

# --- Configuración de página ---
st.set_page_config(
    page_title="Control de Existencias",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
    
)

@st.cache_data(ttl=300)
def cargar_datos():
    """Carga el Excel del ERP, ejecuta el ETL y evalúa las alertas.

    Función cacheada con TTL de 5 minutos. Toda la cadena de procesamiento
    (extracción → transformación → evaluación de alertas) se ejecuta en una
    sola llamada para aprovechar el cache de Streamlit.

    Returns:
        Tupla (df, df_alertas, config):
        - df (pd.DataFrame | None): DataFrame procesado con todas las columnas
          calculadas. None si el Excel no existe.
        - df_alertas (pd.DataFrame | None): DataFrame de alertas disparadas.
          None si el Excel no existe.
        - config (dict): Configuración cargada.

    Dependencies:
        - Usa: load_config(), extraer_datos_erp(), transformar_datos(),
          evaluar_alertas()
        - Ruta Excel: config.paths.raw_data / config.paths.excel_filename
    """
    config = load_config()
    raw_dir = PROJECT_ROOT / config["paths"]["raw_data"]
    excel_path = raw_dir / config["paths"]["excel_filename"]

    if not excel_path.exists():
        return None, None, config

    datos = extraer_datos_erp(excel_path)
    df = transformar_datos(datos, config)
    df_alertas = evaluar_alertas(df, config)
    return df, df_alertas, config


# --- Cargar datos ---
df, df_alertas, config = cargar_datos()

if df is None:
    st.error("No se encontró el archivo Excel del ERP. Colócalo en data/raw/")
    st.stop()

# --- Header ---
fecha_act = df["fecha_actualizacion"].iloc[0]
col_h1, col_h2 = st.columns([3, 1])
with col_h1:
    st.title("📊 Control de Existencias")
with col_h2:
    st.markdown(
        f"**Última actualización:** {fecha_act.strftime('%d/%m/%Y')}<br>"
        f"**Artículos:** {len(df)}",
        unsafe_allow_html=True,
    )

st.divider()

# --- KPIs ---
n_crit = len(df_alertas[df_alertas["nivel"] == "CRITICA"]["articulo"].unique()) if not df_alertas.empty else 0
n_risk = len(df_alertas[df_alertas["nivel"] == "RIESGO"]["articulo"].unique()) if not df_alertas.empty else 0
n_info = len(df_alertas[df_alertas["nivel"] == "INFORMATIVA"]["articulo"].unique()) if not df_alertas.empty else 0
n_ok = len(df) - len(df_alertas["articulo"].unique()) if not df_alertas.empty else len(df)
cobertura_media = df["semanas_stock"].dropna().mean()

k1, k2, k3, k4 = st.columns(4)
k1.metric("🔴 Artículos Críticos", n_crit, help="Artículos con al menos una alerta crítica (A1–A4)")
k2.metric("🟡 Artículos en Riesgo", n_risk, help="Artículos con al menos una alerta de riesgo (A5–A7)")
k3.metric("🟢 Stock Correcto", max(0, n_ok), help="Artículos sin ninguna alerta activa")
k4.metric("📦 Cobertura Media", f"{cobertura_media:.1f} sem" if pd.notna(cobertura_media) else "N/D", help="Media de semanas de stock de todos los artículos")

st.divider()

# --- Filtros ---
col_f1, col_f2, col_f3 = st.columns([1, 1, 2])
with col_f1:
    filtro_estado = st.selectbox(
        "Estado", ["Todos", "Solo críticos", "Solo riesgo", "Solo OK"],
    )
with col_f2:
    proveedores = sorted(df["nombre_proveedor"].dropna().unique().tolist())
    proveedores = [p for p in proveedores if p.strip()]
    filtro_prov = st.selectbox("Proveedor", ["Todos"] + proveedores)
with col_f3:
    filtro_busqueda = st.text_input("Buscar artículo", "")

# --- Tabla de artículos ---
df_tabla = df[[
    "articulo", "denominacion", "existencia_real", "semanas_stock",
    "stock_minimo", "stock_maximo", "stock_teorico", "nombre_proveedor",
]].copy()

# Añadir columna de estado
# Normalizar artículos para que la comparación funcione aunque haya diferencias de espacios
df_tabla["_art_norm"] = df_tabla["articulo"].astype(str).str.strip()

if not df_alertas.empty:
    df_alertas["_art_norm"] = df_alertas["articulo"].astype(str).str.strip()
    arts_criticos = set(df_alertas[df_alertas["nivel"] == "CRITICA"]["_art_norm"])
    arts_riesgo = set(df_alertas[df_alertas["nivel"] == "RIESGO"]["_art_norm"])
    alertas_por_art = df_alertas.groupby("_art_norm")["alerta_id"].apply(
        lambda x: ", ".join(sorted(x.unique()))
    ).to_dict()
else:
    arts_criticos, arts_riesgo, alertas_por_art = set(), set(), {}

df_tabla["estado"] = df_tabla["_art_norm"].apply(
    lambda x: "🔴 Crítico" if x in arts_criticos
    else ("🟡 Riesgo" if x in arts_riesgo else "🟢 OK")
)
df_tabla["alertas"] = df_tabla["_art_norm"].map(
    lambda x: alertas_por_art.get(x, "—")
)

# Aplicar filtros
if filtro_estado == "Solo críticos":
    df_tabla = df_tabla[df_tabla["_art_norm"].isin(arts_criticos)]
elif filtro_estado == "Solo riesgo":
    df_tabla = df_tabla[df_tabla["_art_norm"].isin(arts_riesgo)]
elif filtro_estado == "Solo OK":
    df_tabla = df_tabla[~df_tabla["_art_norm"].isin(arts_criticos | arts_riesgo)]

if filtro_prov != "Todos":
    df_tabla = df_tabla[df_tabla["nombre_proveedor"] == filtro_prov]

if filtro_busqueda:
    mask = (
        df_tabla["articulo"].str.contains(filtro_busqueda, case=False, na=False)
        | df_tabla["denominacion"].str.contains(filtro_busqueda, case=False, na=False)
    )
    df_tabla = df_tabla[mask]

# Ordenar: críticos primero
orden = {"🔴 Crítico": 0, "🟡 Riesgo": 1, "🟢 OK": 2}
df_tabla["_orden"] = df_tabla["estado"].map(orden)
df_tabla = df_tabla.sort_values(["_orden", "semanas_stock"], ascending=[True, True])
df_tabla = df_tabla.drop(columns=["_orden", "_art_norm"])

st.dataframe(
    df_tabla.rename(columns={
        "articulo": "Artículo",
        "denominacion": "Denominación",
        "existencia_real": "Exist. Real",
        "semanas_stock": "Sem. Stock",
        "stock_minimo": "Stock Mín.",
        "stock_maximo": "Stock Máx.",
        "stock_teorico": "Stock Teórico",
        "nombre_proveedor": "Proveedor",
        "estado": "Estado",
        "alertas": "Alertas",
    }),
    use_container_width=True,
    hide_index=True,
    height=400,
)

st.divider()

# --- Gráficos ---
col_g1, col_g2 = st.columns(2)

with col_g1:
    st.subheader("Cobertura de stock por artículo")
    df_cob = df[["articulo", "semanas_stock"]].dropna().sort_values("semanas_stock")
    df_cob["color"] = df_cob["semanas_stock"].apply(
        lambda x: "Crítico (< 1 sem)" if x < 1
        else ("Riesgo (1-3 sem)" if x < 3 else "OK (> 3 sem)")
    )
    fig = px.bar(
        df_cob, x="articulo", y="semanas_stock", color="color",
        color_discrete_map={
            "Crítico (< 1 sem)": "#E24B4A",
            "Riesgo (1-3 sem)": "#EF9F27",
            "OK (> 3 sem)": "#639922",
        },
        labels={"semanas_stock": "Semanas", "articulo": "Artículo", "color": "Estado"},
    )
    fig.add_hline(y=1, line_dash="dash", line_color="red", opacity=0.5,
                  annotation_text="Umbral mínimo (1 sem)")
    fig.update_layout(height=350, showlegend=True, margin=dict(t=20, b=20))
    st.plotly_chart(fig, use_container_width=True)

with col_g2:
    st.subheader("Distribución de alertas")
    if not df_alertas.empty:
        conteo = df_alertas.groupby("alerta_id").size().reset_index(name="conteo")
        conteo["nombre"] = conteo["alerta_id"].map(
            lambda x: f"{x} — {config.get('alerts', {}).get(x, {}).get('nombre', '')}"
        )
        conteo["nivel"] = conteo["alerta_id"].map(
            lambda x: config.get("alerts", {}).get(x, {}).get("nivel", "")
        )
        conteo["color"] = conteo["nivel"].map({
            "CRITICA": "#E24B4A", "RIESGO": "#EF9F27", "INFORMATIVA": "#378ADD"
        })
        fig2 = px.bar(
            conteo, y="nombre", x="conteo", orientation="h",
            color="nivel",
            color_discrete_map={
                "CRITICA": "#E24B4A", "RIESGO": "#EF9F27", "INFORMATIVA": "#378ADD"
            },
            labels={"conteo": "Nº alertas", "nombre": "", "nivel": "Nivel"},
        )
        fig2.update_layout(height=350, showlegend=True, margin=dict(t=20, b=20))
        st.plotly_chart(fig2, use_container_width=True)
    else:
        st.success("Sin alertas activas")

# --- Pedidos en tránsito ---
df_transito = df[df["pendiente_recibir"] > 0][
    ["articulo", "denominacion", "pendiente_recibir", "nombre_proveedor", "numero_pedido"]
].copy()
if not df_transito.empty:
    st.subheader("Pedidos en tránsito")
    st.dataframe(
        df_transito.rename(columns={
            "articulo": "Artículo", "denominacion": "Denominación",
            "pendiente_recibir": "Cantidad", "nombre_proveedor": "Proveedor",
            "numero_pedido": "Nº Pedido",
        }),
        use_container_width=True, hide_index=True,
    )
