"""
Página 5: Proveedores
Vista por proveedor, fiabilidad, comparativa de precios.
"""
import sys
from pathlib import Path
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import streamlit as st
import pandas as pd
import plotly.express as px

from src.utils.config_loader import load_config, is_page_enabled
from src.etl.etl_existencias import extraer_datos_erp, transformar_datos
from src.alerts.rules_existencias import evaluar_alertas

config = load_config()
if not is_page_enabled(config, "proveedores"):
    st.warning("Esta página está desactivada desde el panel de administración.")
    st.stop()

st.title("🏭 Proveedores")


@st.cache_data(ttl=300)
def cargar_datos():
    raw_dir = PROJECT_ROOT / config["paths"]["raw_data"]
    excel_path = raw_dir / config["paths"]["excel_filename"]
    if not excel_path.exists():
        return None, None, None
    datos = extraer_datos_erp(excel_path)
    df = transformar_datos(datos, config)
    df_alertas = evaluar_alertas(df, config)
    proveedores = datos.get("proveedores", pd.DataFrame())
    return df, df_alertas, proveedores


df, df_alertas, df_proveedores = cargar_datos()
if df is None:
    st.error("No se encontró el archivo Excel del ERP.")
    st.stop()

# Construir tabla resumen por proveedor
prov_col = "nombre_proveedor"
df_prov_data = df[[prov_col, "articulo", "pendiente_recibir", "semanas_entrega_prov"]].copy()
df_prov_data = df_prov_data[df_prov_data[prov_col].notna() & (df_prov_data[prov_col].str.strip() != "")]

resumen = df_prov_data.groupby(prov_col).agg(
    num_articulos=("articulo", "nunique"),
    pnd_recibir=("pendiente_recibir", "sum"),
    sem_entrega=("semanas_entrega_prov", "first"),
).reset_index()

# Contar alertas por proveedor
if not df_alertas.empty:
    crit_por_prov = df_alertas[df_alertas["nivel"] == "CRITICA"].groupby("proveedor").size().to_dict()
    risk_por_prov = df_alertas[df_alertas["nivel"] == "RIESGO"].groupby("proveedor").size().to_dict()
else:
    crit_por_prov, risk_por_prov = {}, {}

resumen["alertas_criticas"] = resumen[prov_col].map(crit_por_prov).fillna(0).astype(int)
resumen["alertas_riesgo"] = resumen[prov_col].map(risk_por_prov).fillna(0).astype(int)
resumen["estado"] = resumen.apply(
    lambda r: "🔴 Riesgo alto" if r["alertas_criticas"] > 0
    else ("🟡 Riesgo medio" if r["alertas_riesgo"] > 0 else "🟢 OK"),
    axis=1,
)
resumen = resumen.sort_values("alertas_criticas", ascending=False)

# KPIs
st.divider()
k1, k2, k3, k4 = st.columns(4)
k1.metric("Proveedores activos", len(resumen))
k2.metric("Pedidos en tránsito", int(resumen["pnd_recibir"].sum()))
k3.metric("Entrega media", f"{resumen['sem_entrega'].mean():.1f} sem")
k4.metric("Con riesgo", len(resumen[resumen["alertas_criticas"] > 0]))

st.divider()

# Tabla resumen
st.subheader("Resumen de proveedores")
st.dataframe(
    resumen.rename(columns={
        prov_col: "Proveedor", "num_articulos": "Artículos",
        "pnd_recibir": "Pnd. Recibir", "sem_entrega": "Sem. Entrega",
        "alertas_criticas": "Críticas", "alertas_riesgo": "Riesgo",
        "estado": "Estado",
    }),
    use_container_width=True, hide_index=True,
)

# Fichas expandibles
st.divider()
st.subheader("Detalle por proveedor")
for _, prov_row in resumen.iterrows():
    nombre = prov_row[prov_col]
    n_crit = prov_row["alertas_criticas"]
    n_risk = prov_row["alertas_riesgo"]
    emoji = "🔴" if n_crit > 0 else ("🟡" if n_risk > 0 else "🟢")

    with st.expander(f"{emoji} {nombre} — {prov_row['num_articulos']} artículos"):
        df_arts = df[df[prov_col] == nombre][[
            "articulo", "denominacion", "existencia_real",
            "stock_minimo", "pendiente_recibir",
        ]].copy()

        # Añadir alertas
        if not df_alertas.empty:
            alertas_map = df_alertas[df_alertas["proveedor"] == nombre].groupby("articulo")["alerta_id"].apply(
                lambda x: ", ".join(sorted(x.unique()))
            ).to_dict()
        else:
            alertas_map = {}

        df_arts["alertas"] = df_arts["articulo"].map(lambda x: alertas_map.get(x, "—"))

        st.dataframe(
            df_arts.rename(columns={
                "articulo": "Artículo", "denominacion": "Denominación",
                "existencia_real": "Exist. Real", "stock_minimo": "Stock Mín.",
                "pendiente_recibir": "Pnd. Recibir", "alertas": "Alertas",
            }),
            use_container_width=True, hide_index=True,
        )

# Gráficos
st.divider()
col_g1, col_g2 = st.columns(2)

with col_g1:
    st.subheader("Semanas de entrega")
    df_ent = resumen.sort_values("sem_entrega")
    df_ent["color"] = df_ent["sem_entrega"].apply(
        lambda x: "≤ 2 sem" if x <= 2 else ("3-4 sem" if x <= 4 else "≥ 5 sem")
    )
    fig = px.bar(
        df_ent, x=prov_col, y="sem_entrega", color="color",
        color_discrete_map={"≤ 2 sem": "#639922", "3-4 sem": "#EF9F27", "≥ 5 sem": "#E24B4A"},
        labels={"sem_entrega": "Semanas", prov_col: "Proveedor", "color": "Tramo"},
    )
    fig.update_layout(height=350, margin=dict(t=20, b=20))
    st.plotly_chart(fig, use_container_width=True)

with col_g2:
    st.subheader("Concentración de artículos")
    fig2 = px.bar(
        resumen.sort_values("num_articulos", ascending=True),
        x="num_articulos", y=prov_col, orientation="h",
        labels={"num_articulos": "Nº artículos", prov_col: ""},
        color_discrete_sequence=["#378ADD"],
    )
    fig2.update_layout(height=350, margin=dict(t=20, b=20))
    st.plotly_chart(fig2, use_container_width=True)

    # Alerta de concentración
    if not resumen.empty:
        total_arts = resumen["num_articulos"].sum()
        for _, r in resumen.iterrows():
            pct = r["num_articulos"] / total_arts if total_arts > 0 else 0
            if pct > 0.25 and r["sem_entrega"] > 4:
                st.warning(
                    f"⚠️ **{r[prov_col]}** concentra el {pct:.0%} de los artículos "
                    f"con {r['sem_entrega']:.0f} semanas de entrega. Riesgo de dependencia."
                )
