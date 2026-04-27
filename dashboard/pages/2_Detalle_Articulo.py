"""
Página 3: Detalle por Artículo
Ficha completa con stock, consumo, pedidos y alertas de un artículo concreto.
"""
import sys
from pathlib import Path
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from datetime import datetime

from src.utils.config_loader import load_config, is_page_enabled
from src.etl.etl_existencias import extraer_datos_erp, transformar_datos
from src.alerts.rules_existencias import evaluar_alertas

config = load_config()
if not is_page_enabled(config, "detalle_articulo"):
    st.warning("Esta página está desactivada desde el panel de administración.")
    st.stop()

st.title("🔍 Detalle por Artículo")


@st.cache_data(ttl=300)
def cargar_datos():
    raw_dir = PROJECT_ROOT / config["paths"]["raw_data"]
    excel_path = raw_dir / config["paths"]["excel_filename"]
    if not excel_path.exists():
        return None, None
    datos = extraer_datos_erp(excel_path)
    df = transformar_datos(datos, config)
    df_alertas = evaluar_alertas(df, config)
    return df, df_alertas


df, df_alertas = cargar_datos()
if df is None:
    st.error("No se encontró el archivo Excel del ERP.")
    st.stop()

# Selector
opciones = df.apply(lambda r: f"{r['articulo']} — {r['denominacion']}", axis=1).tolist()
seleccion = st.selectbox("Selecciona un artículo", opciones)
art_codigo = seleccion.split(" — ")[0].strip()
row = df[df["articulo"] == art_codigo].iloc[0]

st.divider()

# KPIs rápidos
k1, k2, k3, k4 = st.columns(4)
sem_stock = row.get("semanas_stock")
k1.metric("Existencia Real", f"{row['existencia_real']:,.0f}")
k2.metric("Cobertura", f"{sem_stock:.1f} sem" if pd.notna(sem_stock) else "N/D")
k3.metric("Stock Teórico", f"{row['stock_teorico']:,.0f}")
k4.metric("Pnd. Recibir", f"{row['pendiente_recibir']:,.0f}")

st.divider()

# Dos columnas: ficha + alertas
col_izq, col_der = st.columns(2)

with col_izq:
    st.subheader("Datos del artículo")
    ficha = {
        "Código": art_codigo,
        "Denominación": row.get("denominacion", ""),
        "Proveedor habitual": row.get("proveedor_habitual", row.get("nombre_proveedor", "")),
        "Sem. entrega proveedor": f"{row.get('semanas_entrega_prov', 0):.0f} semanas",
        "Pedido mínimo": f"{row.get('pedido_minimo', 0):,.0f}",
        "Stock mínimo": f"{row['stock_minimo']:,.0f}",
        "Stock máximo": f"{row['stock_maximo']:,.0f}",
        "Nº pedido en curso": str(row.get("numero_pedido", "")),
    }
    f_ped = row.get("fecha_ultimo_pedido")
    if pd.notna(f_ped) and isinstance(f_ped, datetime):
        ficha["Último pedido"] = f_ped.strftime("%d/%m/%Y")

    for label, val in ficha.items():
        st.markdown(f"**{label}:** {val}")

    # Gauge de posición de stock
    st.subheader("Posición de stock")
    s_min = row["stock_minimo"]
    s_max = row["stock_maximo"]
    s_real = row["existencia_real"]
    s_teorico = row["stock_teorico"]
    tope = max(s_max * 1.1, s_teorico * 1.1, s_real * 1.1, 1)

    fig_gauge = go.Figure()
    fig_gauge.add_trace(go.Bar(
        x=[s_real], y=["Stock"], orientation="h", name="Existencia real",
        marker_color="#378ADD", text=[f"{s_real:,.0f}"], textposition="outside"
    ))
    fig_gauge.add_trace(go.Bar(
        x=[s_teorico - s_real if s_teorico > s_real else 0], y=["Stock"],
        orientation="h", name="+ Pendiente (teórico)",
        marker_color="#639922", opacity=0.5,
    ))
    fig_gauge.add_vline(x=s_min, line_dash="dash", line_color="red",
                        annotation_text=f"Mín: {s_min:,.0f}")
    fig_gauge.add_vline(x=s_max, line_dash="dash", line_color="gray",
                        annotation_text=f"Máx: {s_max:,.0f}")
    fig_gauge.update_layout(
        barmode="stack", height=120, showlegend=True,
        xaxis=dict(range=[0, tope]), yaxis=dict(visible=False),
        margin=dict(t=30, b=10, l=10, r=10),
    )
    st.plotly_chart(fig_gauge, use_container_width=True)

with col_der:
    st.subheader("Alertas activas")
    if df_alertas is not None and not df_alertas.empty:
        alertas_art = df_alertas[df_alertas["articulo"] == art_codigo]
        if not alertas_art.empty:
            for _, alerta in alertas_art.iterrows():
                nivel = alerta["nivel"]
                emoji = "🔴" if nivel == "CRITICA" else ("🟡" if nivel == "RIESGO" else "🔵")
                st.markdown(
                    f"{emoji} **{alerta['alerta_id']}** — {alerta['nombre']}  \n"
                    f"Valor: `{alerta['valor']}` | Umbral: `{alerta['umbral']}`"
                )
        else:
            st.success("Sin alertas activas para este artículo")
    else:
        st.success("Sin alertas activas")

    # Observaciones
    obs = row.get("observaciones", "")
    if obs and obs.strip() and obs.lower() != "nan":
        st.subheader("Observaciones")
        if "cambio" in obs.lower():
            st.warning(f"⚠️ {obs}")
        else:
            st.info(obs)

# --- Consumo histórico ---
st.divider()
st.subheader("Consumo semanal histórico")
hist_path = PROJECT_ROOT / config["paths"]["processed_data"] / "historico_consumo.csv"
if hist_path.exists():
    df_hist = pd.read_csv(hist_path, dtype=str)
    df_art = df_hist[df_hist["articulo"] == art_codigo].copy()
    if not df_art.empty:
        df_art["consumo_real"] = pd.to_numeric(df_art["consumo_real"], errors="coerce")
        df_art["consumo_escandallo"] = pd.to_numeric(df_art["consumo_escandallo"], errors="coerce")
        df_art = df_art.sort_values("fecha").tail(24)

        fig_consumo = go.Figure()
        fig_consumo.add_trace(go.Bar(
            x=df_art["fecha"], y=df_art["consumo_real"],
            name="Consumo real", marker_color="#378ADD",
        ))
        esc_val = df_art["consumo_escandallo"].iloc[-1] if len(df_art) > 0 else 0
        fig_consumo.add_hline(y=esc_val, line_dash="dash", line_color="#E24B4A",
                              annotation_text=f"Escandallo: {esc_val:,.0f}")
        fig_consumo.update_layout(height=300, margin=dict(t=30, b=20))
        st.plotly_chart(fig_consumo, use_container_width=True)
    else:
        st.info("Historial en construcción — más datos disponibles en próximas ejecuciones.")
else:
    st.info("Sin historial de consumo. Ejecuta run_jobs.py para generar datos.")
