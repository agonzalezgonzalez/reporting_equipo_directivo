"""
Página 4: Evolución y Tendencias
Gráficos temporales, proyecciones y mapa de calor de alertas.
"""
import sys
from pathlib import Path
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from styles import aplicar_estilos, mostrar_logo_sidebar
aplicar_estilos()
mostrar_logo_sidebar()

from src.utils.config_loader import load_config, is_page_enabled

config = load_config()
if not is_page_enabled(config, "evolucion"):
    st.warning("Esta página está desactivada desde el panel de administración.")
    st.stop()

st.title("📈 Evolución y Tendencias")

processed_dir = PROJECT_ROOT / config["paths"]["processed_data"]

# --- Cargar históricos ---
@st.cache_data(ttl=300)
def cargar_historicos():
    data = {}
    for name in ["historico_stock", "historico_alertas", "historico_consumo"]:
        fpath = processed_dir / f"{name}.csv"
        if fpath.exists():
            data[name] = pd.read_csv(fpath, dtype=str)
        else:
            data[name] = pd.DataFrame()
    return data

historicos = cargar_historicos()
df_stock = historicos["historico_stock"]
df_alertas_h = historicos["historico_alertas"]
df_consumo_h = historicos["historico_consumo"]

if df_stock.empty:
    st.info("Sin datos históricos. Ejecuta run_jobs.py varias veces para generar el historial.")
    st.stop()

# Filtros
col_f1, col_f2, col_f3 = st.columns(3)
articulos = sorted(df_stock["articulo"].unique().tolist())
with col_f1:
    f_art = st.selectbox("Artículo", ["Todos"] + articulos)
with col_f2:
    f_periodo = st.selectbox("Periodo", ["Últimas 12 semanas", "Últimas 24 semanas", "Último año"])
with col_f3:
    f_agrup = st.selectbox("Agrupación", ["Semanal", "Mensual"])

# Preparar datos
df_s = df_stock.copy()
for col in ["existencia_real", "stock_minimo", "stock_maximo", "stock_teorico",
            "consumo_escandallo", "semanas_stock", "pendiente_recibir"]:
    if col in df_s.columns:
        df_s[col] = pd.to_numeric(df_s[col], errors="coerce")

df_s["fecha"] = pd.to_datetime(df_s["fecha"], errors="coerce")
df_s = df_s.dropna(subset=["fecha"]).sort_values("fecha")

# Filtrar periodo
max_fecha = df_s["fecha"].max()
if f_periodo == "Últimas 12 semanas":
    df_s = df_s[df_s["fecha"] >= max_fecha - pd.Timedelta(weeks=12)]
elif f_periodo == "Últimas 24 semanas":
    df_s = df_s[df_s["fecha"] >= max_fecha - pd.Timedelta(weeks=24)]

if f_art != "Todos":
    df_s = df_s[df_s["articulo"] == f_art]

if df_s.empty:
    st.warning("Sin datos para los filtros seleccionados.")
    st.stop()

st.divider()

# --- KPIs de tendencia ---
k1, k2, k3, k4 = st.columns(4)
consumo_medio = df_s.groupby("fecha")["existencia_real"].sum().mean()
cobertura_media = df_s["semanas_stock"].dropna().mean()
k1.metric("Consumo medio semanal", f"{df_s['consumo_escandallo'].mean():,.0f}")
k2.metric("Desviación media escandallo", "Ver gráfico")
k3.metric("Cobertura media", f"{cobertura_media:.1f} sem" if pd.notna(cobertura_media) else "N/D")

if not df_alertas_h.empty:
    df_ah = df_alertas_h.copy()
    df_ah["fecha"] = pd.to_datetime(df_ah["fecha"], errors="coerce")
    df_ah = df_ah[df_ah["fecha"] >= max_fecha - pd.Timedelta(weeks=12)]
    k4.metric("Alertas acumuladas", len(df_ah))
else:
    k4.metric("Alertas acumuladas", 0)

st.divider()

# --- Gráfico: Stock teórico vs mínimo ---
st.subheader("Stock teórico vs. mínimo")
if f_art != "Todos":
    df_chart = df_s.groupby("fecha").agg(
        stock_teorico=("stock_teorico", "mean"),
        stock_minimo=("stock_minimo", "mean"),
    ).reset_index()
else:
    df_chart = df_s.groupby("fecha").agg(
        stock_teorico=("stock_teorico", "sum"),
        stock_minimo=("stock_minimo", "sum"),
    ).reset_index()

fig1 = go.Figure()
fig1.add_trace(go.Scatter(
    x=df_chart["fecha"], y=df_chart["stock_teorico"],
    mode="lines+markers", name="Stock teórico",
    line=dict(color="#378ADD", width=2), fill="tozeroy",
    fillcolor="rgba(55, 138, 221, 0.1)",
))
fig1.add_trace(go.Scatter(
    x=df_chart["fecha"], y=df_chart["stock_minimo"],
    mode="lines", name="Stock mínimo",
    line=dict(color="#E24B4A", width=1.5, dash="dash"),
))

# Proyección simple
if len(df_chart) >= 3:
    x_num = np.arange(len(df_chart))
    y_vals = df_chart["stock_teorico"].values
    mask = ~np.isnan(y_vals)
    if mask.sum() >= 2:
        coefs = np.polyfit(x_num[mask], y_vals[mask], 1)
        x_proj = np.arange(len(df_chart), len(df_chart) + 3)
        y_proj = np.polyval(coefs, x_proj)
        fechas_proj = pd.date_range(df_chart["fecha"].max() + pd.Timedelta(weeks=1), periods=3, freq="W")
        fig1.add_trace(go.Scatter(
            x=fechas_proj, y=y_proj,
            mode="lines+markers", name="Proyección",
            line=dict(color="#EF9F27", width=1.5, dash="dot"),
            marker=dict(size=5, color="#EF9F27"),
        ))

fig1.update_layout(height=350, margin=dict(t=20, b=20))
st.plotly_chart(fig1, use_container_width=True)

# --- Gráficos gemelos ---
col_g1, col_g2 = st.columns(2)

with col_g1:
    st.subheader("Consumo real vs. escandallo")
    if not df_consumo_h.empty:
        df_c = df_consumo_h.copy()
        df_c["consumo_real"] = pd.to_numeric(df_c["consumo_real"], errors="coerce")
        df_c["consumo_escandallo"] = pd.to_numeric(df_c["consumo_escandallo"], errors="coerce")
        df_c["fecha"] = pd.to_datetime(df_c["fecha"], errors="coerce")
        if f_art != "Todos":
            df_c = df_c[df_c["articulo"] == f_art]
        df_c = df_c.dropna(subset=["fecha"]).sort_values("fecha")

        if not df_c.empty:
            df_cg = df_c.groupby("fecha").agg(
                consumo_real=("consumo_real", "sum"),
                consumo_escandallo=("consumo_escandallo", "sum"),
            ).reset_index()

            fig2 = go.Figure()
            fig2.add_trace(go.Scatter(
                x=df_cg["fecha"], y=df_cg["consumo_real"],
                name="Consumo real", line=dict(color="#378ADD"),
            ))
            fig2.add_trace(go.Scatter(
                x=df_cg["fecha"], y=df_cg["consumo_escandallo"],
                name="Escandallo", line=dict(color="#639922"),
            ))
            fig2.update_layout(height=300, margin=dict(t=20, b=20))
            st.plotly_chart(fig2, use_container_width=True)
        else:
            st.info("Sin datos de consumo para los filtros seleccionados.")
    else:
        st.info("Sin historial de consumo.")

with col_g2:
    st.subheader("Evolución de cobertura")
    df_cob = df_s[["fecha", "semanas_stock"]].dropna()
    if not df_cob.empty:
        df_cob_g = df_cob.groupby("fecha")["semanas_stock"].mean().reset_index()
        fig3 = go.Figure()
        fig3.add_trace(go.Scatter(
            x=df_cob_g["fecha"], y=df_cob_g["semanas_stock"],
            mode="lines+markers", name="Cobertura",
            line=dict(color="#1D9E75", width=2), fill="tozeroy",
            fillcolor="rgba(29, 158, 117, 0.1)",
        ))
        fig3.add_hline(y=1, line_dash="dash", line_color="red", opacity=0.5,
                       annotation_text="Umbral mínimo (1 sem)")
        fig3.update_layout(height=300, margin=dict(t=20, b=20))
        st.plotly_chart(fig3, use_container_width=True)
    else:
        st.info("Sin datos de cobertura.")

# --- Mapa de calor ---
st.divider()
st.subheader("Mapa de calor: alertas por artículo y semana")
if not df_alertas_h.empty:
    df_hm = df_alertas_h.copy()
    df_hm["fecha"] = pd.to_datetime(df_hm["fecha"], errors="coerce")
    df_hm = df_hm.dropna(subset=["fecha"])
    df_hm["semana_label"] = df_hm["fecha"].dt.strftime("S%W")

    if f_art != "Todos":
        df_hm = df_hm[df_hm["articulo"] == f_art]

    if not df_hm.empty:
        pivot = df_hm.groupby(["articulo", "semana_label"]).size().reset_index(name="count")
        pivot_table = pivot.pivot(index="articulo", columns="semana_label", values="count").fillna(0)

        fig_hm = px.imshow(
            pivot_table, color_continuous_scale="Reds",
            labels=dict(x="Semana", y="Artículo", color="Alertas"),
            aspect="auto",
        )
        fig_hm.update_layout(height=max(200, len(pivot_table) * 30), margin=dict(t=20, b=20))
        st.plotly_chart(fig_hm, use_container_width=True)
    else:
        st.info("Sin alertas para los filtros seleccionados.")
else:
    st.info("Sin historial de alertas.")

# --- Ranking recurrencia ---
if not df_alertas_h.empty:
    st.subheader("Artículos con mayor recurrencia de alertas")
    ranking = df_alertas_h.groupby("articulo").size().reset_index(name="total_alertas")
    ranking = ranking.sort_values("total_alertas", ascending=False).head(10)

    fig_rank = px.bar(
        ranking, x="total_alertas", y="articulo", orientation="h",
        color="total_alertas", color_continuous_scale="Reds",
        labels={"total_alertas": "Total alertas", "articulo": "Artículo"},
    )
    fig_rank.update_layout(
        height=300, showlegend=False, yaxis=dict(autorange="reversed"),
        margin=dict(t=20, b=20),
    )
    st.plotly_chart(fig_rank, use_container_width=True)
