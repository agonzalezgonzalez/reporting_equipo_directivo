"""
Motor de reglas de alerta.

Module: src.alerts.rules_existencias
Purpose: Evalúa las 10 reglas de alerta (A1–A10) definidas en el documento
    funcional sobre el DataFrame de artículos procesado. Cada regla compara
    un valor calculado del artículo contra un umbral configurable y genera
    un registro de alerta si se cumple la condición.
Input: DataFrame procesado por src.etl.etl_existencias.transformar_datos()
Output: DataFrame con una fila por cada alerta disparada
Config: config/settings.yaml (secciones alerts y params)
Used by: run_jobs.py (paso 5), dashboard/app_main.py,
    dashboard/pages/1_Alertas_Activas.py,
    dashboard/pages/2_Detalle_Articulo.py,
    dashboard/pages/4_Proveedores.py

Reglas implementadas:
    A1  (CRITICA)     — Stock cubre menos de N semanas (default 1)
    A2  (CRITICA)     — Pedido llega después de agotamiento (semana_entrega > semanas_stock)
    A3  (CRITICA)     — Stock teórico por debajo del mínimo (stock_vs_minimo < 1)
    A4  (CRITICA)     — Agotamiento estimado en menos de N días (default 30)
    A5  (RIESGO)      — Pedido en ventana de riesgo sin confirmar recepción (default 24 días)
    A6  (RIESGO)      — Pedidos pendientes de recibir (en tránsito)
    A7  (RIESGO)      — Desviación consumo real vs escandallo ≥ N% (default 80%)
    A8  (INFORMATIVA) — Artículo sin consumo configurado en tabla maestra
    A9  (INFORMATIVA) — Observaciones contienen la palabra "cambio"
    A10 (INFORMATIVA) — Sobreestimación del escandallo (desviación ≤ -N%)
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
    """Evalúa las 10 reglas de alerta sobre el DataFrame procesado.

    Recorre cada artículo del DataFrame y comprueba las condiciones de las
    alertas A1–A10 según los umbrales definidos en la configuración. Un mismo
    artículo puede disparar múltiples alertas simultáneamente (ej: A1 y A3).

    Solo genera registros para las alertas que están habilitadas
    (dashboard_enabled=true en settings.yaml). Las alertas deshabilitadas
    se ignoran completamente.

    Args:
        df: DataFrame procesado por transformar_datos() con las columnas
            calculadas necesarias:
            - semanas_stock (float): para A1
            - semana_entrega (float): para A2
            - stock_vs_minimo (float): para A3
            - fecha_agotamiento (datetime): para A4
            - fecha_ultimo_pedido (datetime): para A5
            - pendiente_recibir (float): para A5, A6
            - desviacion_consumo (float): para A7, A10
            - consumo_escandallo (float): para A8
            - en_tabla_maestra (bool): para A8
            - observaciones (str): para A9
            - nombre_proveedor (str): para todas (campo informativo)
            - fecha_actualizacion (datetime): fecha base para cálculos
        config: Diccionario de configuración cargado por load_config().
            Se usan las claves:
            - params.ventana_riesgo_dias (default: 24)
            - params.umbral_agotamiento_dias (default: 30)
            - params.umbral_semanas_stock_critico (default: 1.0)
            - params.umbral_desviacion_consumo (default: 0.80)
            - alerts.{A1..A10}.dashboard_enabled (bool)
            - alerts.{A1..A10}.notification_enabled (bool)
            - alerts.{A1..A10}.channels (list[str])
            - alerts.{A1..A10}.action (str)

    Returns:
        DataFrame con una fila por cada alerta disparada. Columnas:
        - fecha (str): fecha de evaluación en formato YYYY-MM-DD
        - semana (int): número de semana ISO
        - articulo (str): código del artículo
        - denominacion (str): nombre del artículo
        - alerta_id (str): identificador de la alerta (A1–A10)
        - nivel (str): CRITICA | RIESGO | INFORMATIVA
        - nombre (str): descripción corta de la alerta
        - valor (str): dato concreto que disparó la alerta (ej: "0.4 sem")
        - umbral (str): referencia del umbral (ej: "< 1 sem")
        - action (str): acción recomendada desde settings.yaml
        - proveedor (str): nombre del proveedor del artículo
        - notification_enabled (bool): si la alerta debe generar notificación
        - channels (list[str]): canales de envío configurados
        DataFrame vacío con las columnas definidas si no se dispara ninguna alerta.

    Dependencies:
        - Consume la salida de: src.etl.etl_existencias.transformar_datos()
        - Configuración: config/settings.yaml (secciones alerts y params)
        - Usa internamente: _is_enabled(), _build_alert(), ALERT_DEFINITIONS
        - Consumido por: run_jobs.py (paso 5), dashboard/app_main.py,
          dashboard/pages/1_Alertas_Activas.py,
          dashboard/pages/2_Detalle_Articulo.py

    Example:
        >>> config = load_config()
        >>> datos = extraer_datos_erp(excel_path)
        >>> df = transformar_datos(datos, config)
        >>> df_alertas = evaluar_alertas(df, config)
        >>> criticas = df_alertas[df_alertas["nivel"] == "CRITICA"]
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
    """Comprueba si una alerta está habilitada para el dashboard.

    Args:
        alert_config: Sección 'alerts' del diccionario de configuración.
        alert_id: Identificador de la alerta (ej: "A1", "A7").

    Returns:
        True si dashboard_enabled es true o si la alerta no está
        en la configuración (habilitada por defecto).

    Dependencies:
        - Llamada por: evaluar_alertas() antes de evaluar cada regla
        - Lee: config.alerts.{alert_id}.dashboard_enabled
    """
    return alert_config.get(alert_id, {}).get("dashboard_enabled", True)


def _build_alert(
    alert_id: str, articulo: str, denominacion: str,
    valor: str, umbral: str, alert_config: dict,
    fecha: datetime, proveedor: str
) -> dict:
    """Construye un registro de alerta con todos los campos necesarios.

    Combina la información estática de ALERT_DEFINITIONS (nombre, nivel)
    con la configuración dinámica de settings.yaml (action, notification_enabled,
    channels) y los datos del artículo que disparó la alerta.

    El campo 'valor' es una representación en texto del dato que disparó la
    alerta. Su formato varía según el tipo:
    - A1: "0.4 sem" (semanas de stock)
    - A2: "Entrega: 3.2 > Stock: 0.4" (comparación semanas)
    - A3: "-17,269" (diferencia stock teórico - mínimo)
    - A4: "12/04/2026" (fecha de agotamiento)
    - A5: "22/03/2026" (fecha del pedido)
    - A6: "85,050 uds" (cantidad pendiente)
    - A7: "+87%" (desviación porcentual)
    - A8: "NULO" (sin consumo)
    - A9: "cambio" (palabra detectada)
    - A10: "-84%" (desviación negativa)

    Args:
        alert_id: Identificador (A1–A10).
        articulo: Código del artículo.
        denominacion: Nombre del artículo.
        valor: Representación en texto del dato que disparó la alerta.
        umbral: Representación en texto del umbral contra el que se comparó.
        alert_config: Sección 'alerts' de la configuración.
        fecha: Fecha de actualización del ERP (datetime).
        proveedor: Nombre del proveedor del artículo.

    Returns:
        Diccionario con todos los campos de un registro de alerta,
        listo para insertarse en el DataFrame de alertas.

    Dependencies:
        - Llamada por: evaluar_alertas() cuando una condición se cumple
        - Lee: ALERT_DEFINITIONS, config.alerts.{alert_id}
        - Su salida se acumula en el DataFrame que devuelve evaluar_alertas()
        - El campo 'valor' se muestra en: email de alertas (col Valor),
          dashboard/pages/1_Alertas_Activas.py (col Valor),
          dashboard/pages/2_Detalle_Articulo.py (lista de alertas)
    """
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