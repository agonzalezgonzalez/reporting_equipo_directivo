"""
Página 2: Alertas Activas
Detalle de todas las alertas agrupadas por criticidad con acciones recomendadas.
"""
import sys
from pathlib import Path
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import streamlit as st
import pandas as pd
from src.utils.config_loader import load_config, is_page_enabled
from src.etl.etl_existencias import extraer_datos_erp, transformar_datos
from src.alerts.rules_existencias import evaluar_alertas

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from styles import aplicar_estilos, mostrar_logo_sidebar
aplicar_estilos()
mostrar_logo_sidebar()

config = load_config()
if not is_page_enabled(config, "alertas_activas"):
    st.warning("Esta página está desactivada desde el panel de administración.")
    st.stop()

st.title("🚨 Alertas Activas")


@st.cache_data(ttl=300)
def cargar_alertas():
    raw_dir = PROJECT_ROOT / config["paths"]["raw_data"]
    excel_path = raw_dir / config["paths"]["excel_filename"]
    if not excel_path.exists():
        return None
    datos = extraer_datos_erp(excel_path)
    df = transformar_datos(datos, config)
    return evaluar_alertas(df, config)


df_alertas = cargar_alertas()
if df_alertas is None or df_alertas.empty:
    st.success("Sin alertas activas")
    st.stop()

# Resumen
c1, c2, c3 = st.columns(3)
n_crit = len(df_alertas[df_alertas["nivel"] == "CRITICA"])
n_risk = len(df_alertas[df_alertas["nivel"] == "RIESGO"])
n_info = len(df_alertas[df_alertas["nivel"] == "INFORMATIVA"])
c1.metric("🔴 Alertas Críticas", n_crit, help="Total de alertas críticas activas (un artículo puede tener varias)")
c2.metric("🟡 Alertas Riesgo", n_risk, help="Total de alertas de riesgo activas")
c3.metric("🔵 Alertas Informativas", n_info, help="Total de alertas informativas activas")

st.divider()

# Filtros
col_f1, col_f2, col_f3, col_f4 = st.columns([1, 1, 1, 2])
with col_f1:
    f_nivel = st.selectbox("Nivel", ["Todos", "CRITICA", "RIESGO", "INFORMATIVA"])
with col_f2:
    alertas_ids = sorted(df_alertas["alerta_id"].unique().tolist())
    f_tipo = st.selectbox("Tipo alerta", ["Todos"] + alertas_ids)
with col_f3:
    provs = sorted(df_alertas["proveedor"].dropna().unique().tolist())
    provs = [p for p in provs if p.strip()]
    f_prov = st.selectbox("Proveedor", ["Todos"] + provs, key="alerta_prov")
with col_f4:
    f_busq = st.text_input("Buscar artículo", "", key="alerta_busq")

df_filt = df_alertas.copy()
if f_nivel != "Todos":
    df_filt = df_filt[df_filt["nivel"] == f_nivel]
if f_tipo != "Todos":
    df_filt = df_filt[df_filt["alerta_id"] == f_tipo]
if f_prov != "Todos":
    df_filt = df_filt[df_filt["proveedor"] == f_prov]
if f_busq:
    mask = (
        df_filt["articulo"].str.contains(f_busq, case=False, na=False)
        | df_filt["denominacion"].str.contains(f_busq, case=False, na=False)
    )
    df_filt = df_filt[mask]

# Mostrar por grupo
for nivel, emoji, color in [
    ("CRITICA", "🔴", "red"),
    ("RIESGO", "🟡", "orange"),
    ("INFORMATIVA", "🔵", "blue"),
]:
    df_grupo = df_filt[df_filt["nivel"] == nivel]
    if df_grupo.empty:
        continue

    with st.expander(f"{emoji} Alertas {nivel.lower()}s — {len(df_grupo)} alertas", expanded=(nivel == "CRITICA")):
        st.dataframe(
            df_grupo[["alerta_id", "articulo", "denominacion", "valor", "umbral", "action", "proveedor"]].rename(
                columns={
                    "alerta_id": "Alerta", "articulo": "Artículo",
                    "denominacion": "Denominación", "valor": "Valor",
                    "umbral": "Umbral", "action": "Acción Recomendada",
                    "proveedor": "Proveedor",
                }
            ),
            use_container_width=True, hide_index=True,
        )

# Historial de notificaciones
st.divider()
st.subheader("Historial de notificaciones")
log_path = PROJECT_ROOT / config["paths"]["processed_data"] / "log_notificaciones.csv"
if log_path.exists():
    df_log = pd.read_csv(log_path, dtype=str)
    if not df_log.empty:
        df_log = df_log.sort_values("fecha_hora", ascending=False).head(20)
        st.dataframe(
            df_log.rename(columns={
                "fecha_hora": "Fecha/Hora", "canal": "Canal",
                "destinatarios": "Destinatarios", "asunto": "Asunto",
                "num_alertas_criticas": "Críticas", "num_alertas_riesgo": "Riesgo",
                "estado_envio": "Estado",
            }),
            use_container_width=True, hide_index=True,
        )
    else:
        st.info("Sin notificaciones registradas")
else:
    st.info("Sin historial de notificaciones. Ejecuta run_jobs.py para generar datos.")
