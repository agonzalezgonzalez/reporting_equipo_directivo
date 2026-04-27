"""
Módulo de persistencia de datos históricos.
Gestiona la escritura, deduplicación y retención de los 5 almacenes CSV.
"""
import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path

from src.utils.logger import setup_logger

logger = setup_logger("persistence")


HISTORICO_STOCK_COLS = [
    "fecha", "semana", "articulo", "denominacion", "existencia_real",
    "stock_minimo", "stock_maximo", "stock_teorico", "consumo_escandallo",
    "semanas_stock", "pendiente_recibir", "proveedor",
]
HISTORICO_ALERTAS_COLS = [
    "fecha", "semana", "articulo", "alerta_id", "nivel", "valor", "umbral",
]
HISTORICO_CONSUMO_COLS = [
    "fecha", "semana", "articulo", "consumo_real", "consumo_escandallo",
    "desviacion_pct",
]
HISTORICO_PEDIDOS_COLS = [
    "fecha_registro", "articulo", "numero_pedido", "proveedor", "cantidad",
    "fecha_pedido", "fecha_estimada_entrega", "fecha_recepcion_real", "estado",
]
LOG_NOTIFICACIONES_COLS = [
    "fecha_hora", "canal", "destinatarios", "asunto", "num_alertas_criticas",
    "num_alertas_riesgo", "num_articulos", "estado_envio", "detalle_error",
]


def _get_or_create_csv(filepath: Path, columns: list) -> pd.DataFrame:
    """Lee un CSV existente o crea un DataFrame vacío con las columnas indicadas."""
    if filepath.exists() and filepath.stat().st_size > 0:
        try:
            return pd.read_csv(filepath, dtype=str)
        except Exception as e:
            logger.warning(f"Error leyendo {filepath}: {e}. Creando nuevo.")
    return pd.DataFrame(columns=columns)


def guardar_historico_stock(df_procesado: pd.DataFrame, output_dir: Path) -> None:
    """Append de la foto actual de stock al histórico, con deduplicación."""
    filepath = output_dir / "historico_stock.csv"
    df_hist = _get_or_create_csv(filepath, HISTORICO_STOCK_COLS)

    fecha = df_procesado["fecha_actualizacion"].iloc[0].strftime("%Y-%m-%d")
    semana = str(df_procesado["semana_iso"].iloc[0])

    nuevos = []
    for _, row in df_procesado.iterrows():
        nuevos.append({
            "fecha": fecha,
            "semana": semana,
            "articulo": str(row.get("articulo", "")).strip(),
            "denominacion": str(row.get("denominacion", "")).strip(),
            "existencia_real": str(row.get("existencia_real", 0)),
            "stock_minimo": str(row.get("stock_minimo", 0)),
            "stock_maximo": str(row.get("stock_maximo", 0)),
            "stock_teorico": str(row.get("stock_teorico", 0)),
            "consumo_escandallo": str(row.get("consumo_escandallo", 0)),
            "semanas_stock": str(row.get("semanas_stock", "")),
            "pendiente_recibir": str(row.get("pendiente_recibir", 0)),
            "proveedor": str(row.get("nombre_proveedor", row.get("proveedor_habitual", ""))).strip(),
        })

    df_new = pd.DataFrame(nuevos)

    # Deduplicar: eliminar registros existentes con misma fecha+articulo
    if not df_hist.empty:
        df_hist = df_hist[~((df_hist["fecha"] == fecha) & (df_hist["articulo"].isin(df_new["articulo"])))]

    df_hist = pd.concat([df_hist, df_new], ignore_index=True)
    df_hist.to_csv(filepath, index=False)
    logger.info(f"Histórico stock actualizado: {len(df_new)} registros ({filepath})")


def guardar_historico_alertas(df_alertas: pd.DataFrame, output_dir: Path) -> None:
    """Append de alertas disparadas al histórico."""
    filepath = output_dir / "historico_alertas.csv"
    df_hist = _get_or_create_csv(filepath, HISTORICO_ALERTAS_COLS)

    if df_alertas.empty:
        logger.info("Sin alertas para registrar en histórico.")
        return

    fecha = df_alertas["fecha"].iloc[0]

    df_new = df_alertas[["fecha", "semana", "articulo", "alerta_id", "nivel", "valor", "umbral"]].copy()
    df_new = df_new.astype(str)

    # Deduplicar
    if not df_hist.empty:
        df_hist = df_hist[~((df_hist["fecha"] == fecha) & (df_hist["articulo"].isin(df_new["articulo"])))]

    df_hist = pd.concat([df_hist, df_new], ignore_index=True)
    df_hist.to_csv(filepath, index=False)
    logger.info(f"Histórico alertas actualizado: {len(df_new)} registros")


def guardar_historico_consumo(df_procesado: pd.DataFrame, output_dir: Path) -> None:
    """Append del consumo real semanal al histórico."""
    filepath = output_dir / "historico_consumo.csv"
    df_hist = _get_or_create_csv(filepath, HISTORICO_CONSUMO_COLS)

    fecha = df_procesado["fecha_actualizacion"].iloc[0].strftime("%Y-%m-%d")
    semana = str(df_procesado["semana_iso"].iloc[0])

    nuevos = []
    for _, row in df_procesado.iterrows():
        nuevos.append({
            "fecha": fecha,
            "semana": semana,
            "articulo": str(row.get("articulo", "")).strip(),
            "consumo_real": str(row.get("consumo_real_semana", 0)),
            "consumo_escandallo": str(row.get("consumo_escandallo", 0)),
            "desviacion_pct": str(row.get("desviacion_consumo", "")),
        })

    df_new = pd.DataFrame(nuevos)

    if not df_hist.empty:
        df_hist = df_hist[~((df_hist["fecha"] == fecha) & (df_hist["articulo"].isin(df_new["articulo"])))]

    df_hist = pd.concat([df_hist, df_new], ignore_index=True)
    df_hist.to_csv(filepath, index=False)
    logger.info(f"Histórico consumo actualizado: {len(df_new)} registros")


def actualizar_historico_pedidos(df_procesado: pd.DataFrame, output_dir: Path) -> None:
    """
    Actualiza el histórico de pedidos:
    - Inserta nuevos pedidos detectados.
    - Marca como RECIBIDO los que desaparecen de pendientes.
    """
    filepath = output_dir / "historico_pedidos.csv"
    df_hist = _get_or_create_csv(filepath, HISTORICO_PEDIDOS_COLS)

    fecha_hoy = df_procesado["fecha_actualizacion"].iloc[0].strftime("%Y-%m-%d")

    # Pedidos activos en esta ejecución
    pedidos_actuales = set()
    for _, row in df_procesado.iterrows():
        num_pedido = str(row.get("numero_pedido", "")).strip()
        art = str(row.get("articulo", "")).strip()
        pnd = row.get("pendiente_recibir", 0)
        if num_pedido and num_pedido != "0" and num_pedido != "nan" and pnd > 0:
            pedidos_actuales.add((num_pedido, art))

            # Insertar si es nuevo
            if df_hist.empty or not ((df_hist["numero_pedido"] == num_pedido) & (df_hist["articulo"] == art)).any():
                f_pedido = row.get("fecha_ultimo_pedido")
                sem_ent = row.get("semanas_entrega_prov", 0)
                f_est = ""
                if pd.notna(f_pedido) and isinstance(f_pedido, datetime) and sem_ent > 0:
                    f_est = (f_pedido + timedelta(weeks=sem_ent)).strftime("%Y-%m-%d")

                nuevo = pd.DataFrame([{
                    "fecha_registro": fecha_hoy,
                    "articulo": art,
                    "numero_pedido": num_pedido,
                    "proveedor": str(row.get("nombre_proveedor", row.get("proveedor_habitual", ""))).strip(),
                    "cantidad": str(pnd),
                    "fecha_pedido": f_pedido.strftime("%Y-%m-%d") if pd.notna(f_pedido) and isinstance(f_pedido, datetime) else "",
                    "fecha_estimada_entrega": f_est,
                    "fecha_recepcion_real": "",
                    "estado": "EN_TRANSITO",
                }])
                df_hist = pd.concat([df_hist, nuevo], ignore_index=True)

    # Marcar como RECIBIDO los que ya no están en tránsito
    if not df_hist.empty:
        for idx, row in df_hist.iterrows():
            if row["estado"] == "EN_TRANSITO":
                key = (str(row["numero_pedido"]), str(row["articulo"]))
                if key not in pedidos_actuales:
                    df_hist.at[idx, "estado"] = "RECIBIDO"
                    df_hist.at[idx, "fecha_recepcion_real"] = fecha_hoy
                    # Comprobar retraso
                    f_est = row.get("fecha_estimada_entrega", "")
                    if f_est:
                        try:
                            if datetime.strptime(fecha_hoy, "%Y-%m-%d") > datetime.strptime(f_est, "%Y-%m-%d"):
                                df_hist.at[idx, "estado"] = "RETRASADO"
                        except Exception:
                            pass

    df_hist.to_csv(filepath, index=False)
    logger.info(f"Histórico pedidos actualizado: {len(pedidos_actuales)} en tránsito")


def registrar_notificacion(
    output_dir: Path, canal: str, destinatarios: list[str],
    asunto: str, n_criticas: int, n_riesgo: int, n_articulos: int,
    estado: str, error: str = ""
) -> None:
    """Registra una notificación enviada en el log."""
    filepath = output_dir / "log_notificaciones.csv"
    df_log = _get_or_create_csv(filepath, LOG_NOTIFICACIONES_COLS)

    nuevo = pd.DataFrame([{
        "fecha_hora": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "canal": canal,
        "destinatarios": "; ".join(destinatarios),
        "asunto": asunto,
        "num_alertas_criticas": str(n_criticas),
        "num_alertas_riesgo": str(n_riesgo),
        "num_articulos": str(n_articulos),
        "estado_envio": estado,
        "detalle_error": error,
    }])

    df_log = pd.concat([df_log, nuevo], ignore_index=True)
    df_log.to_csv(filepath, index=False)


def aplicar_retencion(output_dir: Path, config: dict) -> None:
    """Elimina registros más antiguos que el periodo de retención configurado."""
    retencion = config.get("retencion", {})
    hoy = datetime.now()

    archivos_retencion = {
        "historico_stock.csv": retencion.get("historico_stock", 52),
        "historico_alertas.csv": retencion.get("historico_alertas", 52),
        "historico_consumo.csv": retencion.get("historico_consumo", 52),
        "historico_pedidos.csv": retencion.get("historico_pedidos", 104),
        "log_notificaciones.csv": retencion.get("log_notificaciones", 52),
    }

    for filename, semanas in archivos_retencion.items():
        filepath = output_dir / filename
        if not filepath.exists():
            continue

        try:
            df = pd.read_csv(filepath, dtype=str)
            col_fecha = "fecha" if "fecha" in df.columns else "fecha_hora" if "fecha_hora" in df.columns else "fecha_registro"
            if col_fecha not in df.columns:
                continue

            limite = (hoy - timedelta(weeks=semanas)).strftime("%Y-%m-%d")
            antes = len(df)
            df = df[df[col_fecha] >= limite]
            eliminados = antes - len(df)

            if eliminados > 0:
                df.to_csv(filepath, index=False)
                logger.info(f"Retención {filename}: {eliminados} registros eliminados (> {semanas} semanas)")
        except Exception as e:
            logger.warning(f"Error aplicando retención a {filename}: {e}")
