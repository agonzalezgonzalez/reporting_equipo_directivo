"""
Panel General — Página principal del dashboard.
Visión ejecutiva con KPIs, tabla de alertas y cobertura global.
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

# --- Configuración de página ---
st.set_page_config(
    page_title="Control de Existencias",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)


@st.cache_data(ttl=300)
def cargar_datos():
    """Carga y procesa los datos. Cache de 5 minutos."""
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
k1.metric("🔴 Alertas Críticas", n_crit)
k2.metric("🟡 Alertas Riesgo", n_risk)
k3.metric("🟢 Stock Correcto", max(0, n_ok))
k4.metric("📦 Cobertura Media", f"{cobertura_media:.1f} sem" if pd.notna(cobertura_media) else "N/D")

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
if not df_alertas.empty:
    arts_criticos = set(df_alertas[df_alertas["nivel"] == "CRITICA"]["articulo"])
    arts_riesgo = set(df_alertas[df_alertas["nivel"] == "RIESGO"]["articulo"])
    alertas_por_art = df_alertas.groupby("articulo")["alerta_id"].apply(
        lambda x: ", ".join(sorted(x.unique()))
    ).to_dict()
else:
    arts_criticos, arts_riesgo, alertas_por_art = set(), set(), {}

df_tabla["estado"] = df_tabla["articulo"].apply(
    lambda x: "🔴 Crítico" if x in arts_criticos
    else ("🟡 Riesgo" if x in arts_riesgo else "🟢 OK")
)
df_tabla["alertas"] = df_tabla["articulo"].map(
    lambda x: alertas_por_art.get(x, "—")
)

# Aplicar filtros
if filtro_estado == "Solo críticos":
    df_tabla = df_tabla[df_tabla["articulo"].isin(arts_criticos)]
elif filtro_estado == "Solo riesgo":
    df_tabla = df_tabla[df_tabla["articulo"].isin(arts_riesgo)]
elif filtro_estado == "Solo OK":
    df_tabla = df_tabla[~df_tabla["articulo"].isin(arts_criticos | arts_riesgo)]

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
df_tabla = df_tabla.drop(columns=["_orden"])

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
