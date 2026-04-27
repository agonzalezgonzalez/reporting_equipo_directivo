"""
Motor de reglas de alerta.
Evalúa las 10 reglas definidas en el documento funcional sobre el DataFrame procesado.
"""
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

from src.utils.logger import setup_logger

logger = setup_logger("alerts_engine")

ALERT_DEFINITIONS = {
    "A1": {"nombre": "Stock < 1 semana", "nivel": "CRITICA"},
    "A2": {"nombre": "Rotura de stock", "nivel": "CRITICA"},
    "A3": {"nombre": "Bajo mínimo", "nivel": "CRITICA"},
    "A4": {"nombre": "Agotamiento < 30 días", "nivel": "CRITICA"},
    "A5": {"nombre": "Pedido sin confirmar", "nivel": "RIESGO"},
    "A6": {"nombre": "En tránsito", "nivel": "RIESGO"},
    "A7": {"nombre": "Desviación consumo ≥ 80%", "nivel": "RIESGO"},
    "A8": {"nombre": "Sin consumo configurado", "nivel": "INFORMATIVA"},
    "A9": {"nombre": "Cambio pendiente", "nivel": "INFORMATIVA"},
    "A10": {"nombre": "Sobreestimación escandallo", "nivel": "INFORMATIVA"},
}


def evaluar_alertas(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    """
    Evalúa las 10 reglas de alerta sobre el DataFrame procesado.

    Returns:
        DataFrame con una fila por cada alerta disparada.
        Columnas: fecha, semana, articulo, denominacion, alerta_id, nivel,
                  nombre, valor, umbral, action, proveedor
    """
    params = config.get("params", {})
    alert_config = config.get("alerts", {})
    fecha_act = df["fecha_actualizacion"].iloc[0] if len(df) > 0 else datetime.now()

    ventana_riesgo = params.get("ventana_riesgo_dias", 24)
    umbral_agotamiento = params.get("umbral_agotamiento_dias", 30)
    umbral_semanas = params.get("umbral_semanas_stock_critico", 1.0)
    umbral_desviacion = params.get("umbral_desviacion_consumo", 0.80)

    alertas = []

    for _, row in df.iterrows():
        art = str(row.get("articulo", "")).strip()
        denom = str(row.get("denominacion", "")).strip()
        prov = str(row.get("nombre_proveedor", row.get("proveedor_habitual", ""))).strip()

        # A1: Stock < umbral semanas (default 1)
        if _is_enabled(alert_config, "A1"):
            sem = row.get("semanas_stock")
            if pd.notna(sem) and sem < umbral_semanas:
                alertas.append(_build_alert(
                    "A1", art, denom, f"{sem:.1f} sem", f"< {umbral_semanas} sem",
                    alert_config, fecha_act, prov
                ))

        # A2: Semana entrega > semanas stock (rotura)
        if _is_enabled(alert_config, "A2"):
            sem_stock = row.get("semanas_stock")
            sem_entrega = row.get("semana_entrega")
            if pd.notna(sem_stock) and pd.notna(sem_entrega) and sem_entrega > sem_stock:
                alertas.append(_build_alert(
                    "A2", art, denom,
                    f"Entrega: {sem_entrega:.1f} > Stock: {sem_stock:.1f}",
                    "Entrega ≤ Stock",
                    alert_config, fecha_act, prov
                ))

        # A3: Stock teórico - mínimo < 1
        if _is_enabled(alert_config, "A3"):
            vs_min = row.get("stock_vs_minimo")
            if pd.notna(vs_min) and vs_min < 1:
                alertas.append(_build_alert(
                    "A3", art, denom, f"{vs_min:,.0f}", "< 1",
                    alert_config, fecha_act, prov
                ))

        # A4: Fecha agotamiento < fecha_act + umbral días
        if _is_enabled(alert_config, "A4"):
            f_agot = row.get("fecha_agotamiento")
            if pd.notna(f_agot) and isinstance(f_agot, datetime):
                limite = fecha_act + timedelta(days=umbral_agotamiento)
                if f_agot < limite:
                    alertas.append(_build_alert(
                        "A4", art, denom,
                        f_agot.strftime("%d/%m/%Y"),
                        f"< {limite.strftime('%d/%m/%Y')} ({umbral_agotamiento}d)",
                        alert_config, fecha_act, prov
                    ))

        # A5: Pedido en ventana de riesgo
        if _is_enabled(alert_config, "A5"):
            f_pedido = row.get("fecha_ultimo_pedido")
            if pd.notna(f_pedido) and isinstance(f_pedido, datetime):
                limite_inf = fecha_act - timedelta(days=ventana_riesgo)
                if limite_inf <= f_pedido <= fecha_act:
                    pnd = row.get("pendiente_recibir", 0)
                    if pnd > 0:
                        alertas.append(_build_alert(
                            "A5", art, denom,
                            f_pedido.strftime("%d/%m/%Y"),
                            f"Ventana {ventana_riesgo}d",
                            alert_config, fecha_act, prov
                        ))

        # A6: Pedidos pendientes de recibir
        if _is_enabled(alert_config, "A6"):
            pnd = row.get("pendiente_recibir", 0)
            if pd.notna(pnd) and pnd > 0:
                alertas.append(_build_alert(
                    "A6", art, denom,
                    f"{pnd:,.0f} uds", "En tránsito",
                    alert_config, fecha_act, prov
                ))

        # A7: Desviación consumo >= umbral (positiva = escandallo subestimado)
        if _is_enabled(alert_config, "A7"):
            desv = row.get("desviacion_consumo")
            if pd.notna(desv) and desv >= umbral_desviacion:
                alertas.append(_build_alert(
                    "A7", art, denom,
                    f"+{desv:.0%}", f"≥ {umbral_desviacion:.0%}",
                    alert_config, fecha_act, prov
                ))

        # A8: Sin consumo configurado
        if _is_enabled(alert_config, "A8"):
            cons_esc = row.get("consumo_escandallo", 0)
            en_tabla = row.get("en_tabla_maestra", True)
            if not en_tabla or cons_esc == 0:
                alertas.append(_build_alert(
                    "A8", art, denom, "NULO", "Sin consumo",
                    alert_config, fecha_act, prov
                ))

        # A9: Observaciones contienen "cambio"
        if _is_enabled(alert_config, "A9"):
            obs = str(row.get("observaciones", "")).lower()
            if "cambio" in obs:
                alertas.append(_build_alert(
                    "A9", art, denom, "cambio", "Contiene 'cambio'",
                    alert_config, fecha_act, prov
                ))

        # A10: Sobreestimación (desviación <= -umbral)
        if _is_enabled(alert_config, "A10"):
            desv = row.get("desviacion_consumo")
            if pd.notna(desv) and desv <= -umbral_desviacion:
                alertas.append(_build_alert(
                    "A10", art, denom,
                    f"{desv:.0%}", f"≤ -{umbral_desviacion:.0%}",
                    alert_config, fecha_act, prov
                ))

    df_alertas = pd.DataFrame(alertas)
    if df_alertas.empty:
        df_alertas = pd.DataFrame(columns=[
            "fecha", "semana", "articulo", "denominacion", "alerta_id",
            "nivel", "nombre", "valor", "umbral", "action", "proveedor"
        ])

    n_crit = len(df_alertas[df_alertas["nivel"] == "CRITICA"])
    n_risk = len(df_alertas[df_alertas["nivel"] == "RIESGO"])
    n_info = len(df_alertas[df_alertas["nivel"] == "INFORMATIVA"])
    logger.info(
        f"Alertas evaluadas: {len(df_alertas)} total "
        f"(🔴 {n_crit} críticas, 🟡 {n_risk} riesgo, 🔵 {n_info} informativas)"
    )

    return df_alertas


def _is_enabled(alert_config: dict, alert_id: str) -> bool:
    """Comprueba si una alerta está habilitada en dashboard."""
    return alert_config.get(alert_id, {}).get("dashboard_enabled", True)


def _build_alert(
    alert_id: str, articulo: str, denominacion: str,
    valor: str, umbral: str, alert_config: dict,
    fecha: datetime, proveedor: str
) -> dict:
    """Construye un registro de alerta."""
    defn = ALERT_DEFINITIONS.get(alert_id, {})
    cfg = alert_config.get(alert_id, {})
    return {
        "fecha": fecha.strftime("%Y-%m-%d"),
        "semana": fecha.isocalendar()[1],
        "articulo": articulo,
        "denominacion": denominacion,
        "alerta_id": alert_id,
        "nivel": defn.get("nivel", "INFORMATIVA"),
        "nombre": defn.get("nombre", ""),
        "valor": valor,
        "umbral": umbral,
        "action": cfg.get("action", ""),
        "proveedor": proveedor,
        "notification_enabled": cfg.get("notification_enabled", False),
        "channels": cfg.get("channels", []),
    }
