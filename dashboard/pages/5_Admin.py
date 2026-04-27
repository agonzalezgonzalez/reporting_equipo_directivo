"""
Página 6: Panel de Administración
Configuración de páginas, alertas, destinatarios y parámetros.
Doble gestión: settings.yaml (base) + panel visual (Streamlit).
"""
import sys
from pathlib import Path
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import streamlit as st
import yaml

from src.utils.config_loader import load_config, save_config, CONFIG_PATH

st.title("⚙️ Panel de Administración")
st.caption("Los cambios se guardan en config/settings.yaml con backup automático.")

config = load_config()

# ============================================================
# BLOQUE 1: Visibilidad de páginas
# ============================================================
st.header("1. Visibilidad de páginas")
st.markdown("Activa o desactiva páginas del dashboard. El Panel General siempre está visible.")

pages_config = config.get("pages", {})
for key, page in pages_config.items():
    if key == "panel_general":
        st.toggle(page.get("label", key), value=True, disabled=True, key=f"page_{key}")
    else:
        pages_config[key]["enabled"] = st.toggle(
            page.get("label", key),
            value=page.get("enabled", True),
            key=f"page_{key}",
        )

st.divider()

# ============================================================
# BLOQUE 2: Configuración de alertas
# ============================================================
st.header("2. Configuración de alertas")
st.markdown("Activa/desactiva alertas para el dashboard y las notificaciones.")

alerts_config = config.get("alerts", {})
for alert_id in sorted(alerts_config.keys(), key=lambda x: int(x[1:])):
    alert = alerts_config[alert_id]
    with st.expander(f"{alert_id} — {alert.get('nombre', '')} ({alert.get('nivel', '')})"):
        col1, col2, col3 = st.columns(3)
        with col1:
            alert["dashboard_enabled"] = st.toggle(
                "Activa en dashboard", value=alert.get("dashboard_enabled", True),
                key=f"dash_{alert_id}",
            )
        with col2:
            alert["notification_enabled"] = st.toggle(
                "Activa para notificación", value=alert.get("notification_enabled", False),
                key=f"notif_{alert_id}",
            )
        with col3:
            canales = alert.get("channels", ["email"])
            opciones_canales = ["email", "whatsapp"]
            alert["channels"] = st.multiselect(
                "Canales", opciones_canales,
                default=[c for c in canales if c in opciones_canales],
                key=f"chan_{alert_id}",
            )
        alert["action"] = st.text_input(
            "Acción recomendada", value=alert.get("action", ""),
            key=f"action_{alert_id}",
        )

st.divider()

# ============================================================
# BLOQUE 3: Destinatarios
# ============================================================
st.header("3. Destinatarios de notificaciones")

recipients = config.get("notifications", {}).get("recipients", [])

for i, recip in enumerate(recipients):
    with st.expander(f"{'✅' if recip.get('active', False) else '⬜'} {recip.get('name', f'Destinatario {i+1}')}"):
        col1, col2 = st.columns(2)
        with col1:
            recip["name"] = st.text_input("Nombre", value=recip.get("name", ""), key=f"rname_{i}")
            recip["email"] = st.text_input("Email", value=recip.get("email", ""), key=f"remail_{i}")
        with col2:
            recip["phone"] = st.text_input("Teléfono", value=recip.get("phone", ""), key=f"rphone_{i}")
            recip["active"] = st.toggle("Activo", value=recip.get("active", True), key=f"ractive_{i}")

        niveles_opciones = ["CRITICA", "RIESGO", "INFORMATIVA"]
        recip["levels"] = st.multiselect(
            "Niveles de alerta",
            niveles_opciones,
            default=[l for l in recip.get("levels", []) if l in niveles_opciones],
            key=f"rlevels_{i}",
        )
        recip["channels"] = st.multiselect(
            "Canales",
            ["email", "whatsapp"],
            default=[c for c in recip.get("channels", ["email"]) if c in ["email", "whatsapp"]],
            key=f"rchan_{i}",
        )

# Botón para añadir destinatario
if st.button("➕ Añadir destinatario"):
    recipients.append({
        "name": "", "email": "", "phone": "",
        "levels": ["CRITICA"], "channels": ["email"], "active": True,
    })
    config["notifications"]["recipients"] = recipients
    save_config(config)
    st.rerun()

st.divider()

# ============================================================
# BLOQUE 4: Parámetros operativos
# ============================================================
st.header("4. Parámetros operativos")

params = config.get("params", {})

col1, col2 = st.columns(2)
with col1:
    params["ventana_riesgo_dias"] = st.number_input(
        "Ventana de riesgo (días)", min_value=1, max_value=90,
        value=params.get("ventana_riesgo_dias", 24), key="p_ventana",
    )
    params["umbral_agotamiento_dias"] = st.number_input(
        "Umbral agotamiento (días)", min_value=7, max_value=180,
        value=params.get("umbral_agotamiento_dias", 30), key="p_agotamiento",
    )
    params["umbral_cobertura_larga_dias"] = st.number_input(
        "Cobertura larga (días)", min_value=30, max_value=365,
        value=params.get("umbral_cobertura_larga_dias", 60), key="p_cobertura",
    )

with col2:
    params["umbral_semanas_stock_critico"] = st.slider(
        "Semanas stock crítico", min_value=0.5, max_value=4.0, step=0.5,
        value=float(params.get("umbral_semanas_stock_critico", 1.0)), key="p_semanas",
    )
    params["umbral_desviacion_consumo"] = st.slider(
        "Desviación consumo", min_value=0.50, max_value=1.00, step=0.05,
        value=float(params.get("umbral_desviacion_consumo", 0.80)), key="p_desviacion",
        format="%.0f%%",
    )

col3, col4 = st.columns(2)
with col3:
    params["rappel_saeco"] = st.number_input(
        "Rappel SAECO (%)", min_value=0.0, max_value=100.0,
        value=float(params.get("rappel_saeco", 0.11)) * 100, key="p_saeco",
    ) / 100
with col4:
    params["rappel_xuber"] = st.number_input(
        "Rappel XUBER (%)", min_value=0.0, max_value=100.0,
        value=float(params.get("rappel_xuber", 0.06)) * 100, key="p_xuber",
    ) / 100

config["params"] = params

st.divider()

# ============================================================
# GUARDADO
# ============================================================
col_save, col_reset = st.columns([1, 1])
with col_save:
    if st.button("💾 Guardar configuración", type="primary", use_container_width=True):
        try:
            save_config(config)
            st.cache_data.clear()
            st.success("Configuración guardada correctamente. Backup creado en settings.yaml.bak.")
        except Exception as e:
            st.error(f"Error guardando configuración: {e}")

with col_reset:
    if st.button("🔄 Recargar desde archivo", use_container_width=True):
        st.cache_data.clear()
        st.rerun()
