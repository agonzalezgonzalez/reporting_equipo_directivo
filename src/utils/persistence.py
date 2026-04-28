"""
Módulo de persistencia de datos históricos.

Module: src.utils.persistence
Purpose: Gestiona la escritura, deduplicación y retención de los 5 almacenes
    CSV que alimentan los gráficos temporales y el historial del dashboard.
    Cada ejecución del ETL (run_jobs.py) añade registros a estos archivos.
Input: DataFrames procesados por el ETL y las alertas evaluadas
Output: Archivos CSV en data/processed/ (append con deduplicación)
Config: config/settings.yaml (sección retencion)
Used by: run_jobs.py (pasos 6, 8, 9)

Almacenes gestionados:
    1. historico_stock.csv     — Foto semanal de stock por artículo
    2. historico_alertas.csv   — Alertas disparadas por artículo y fecha
    3. historico_consumo.csv   — Consumo real semanal por artículo
    4. historico_pedidos.csv   — Pedidos con estado (EN_TRANSITO/RECIBIDO/RETRASADO)
    5. log_notificaciones.csv  — Log de emails y WhatsApp enviados

Deduplicación:
    Clave (fecha, articulo) para stock, alertas y consumo.
    Clave (numero_pedido, articulo) para pedidos.
    Si ya existen registros con la misma clave, se sobreescriben (upsert).

Retención:
    Configurable en settings.yaml por almacén (default: 52 semanas para
    stock/alertas/consumo/log, 104 semanas para pedidos).
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
    """Lee un CSV existente o crea un DataFrame vacío con las columnas indicadas.

    Maneja el caso de arranque en frío (archivo no existe) y el caso de
    archivo corrupto (error de lectura) creando un DataFrame vacío con
    la estructura correcta.

    Args:
        filepath: Ruta al archivo CSV.
        columns: Lista de nombres de columnas para el DataFrame vacío.

    Returns:
        DataFrame con los datos del CSV, o DataFrame vacío con las
        columnas indicadas si el archivo no existe o está corrupto.

    Dependencies:
        - Llamada por: todas las funciones guardar_* y registrar_notificacion()
    """
    if filepath.exists() and filepath.stat().st_size > 0:
        try:
            return pd.read_csv(filepath, dtype=str)
        except Exception as e:
            logger.warning(f"Error leyendo {filepath}: {e}. Creando nuevo.")
    return pd.DataFrame(columns=columns)


def guardar_historico_stock(df_procesado: pd.DataFrame, output_dir: Path) -> None:
    """Registra la foto actual de stock de todos los artículos en el histórico.

    Añade una fila por artículo con su estado actual de stock. Si ya existen
    registros con la misma fecha + artículo (ejecución duplicada), los
    sobreescribe para evitar duplicados.

    Args:
        df_procesado: DataFrame procesado por transformar_datos(). Se extraen:
            fecha_actualizacion, semana_iso, articulo, denominacion,
            existencia_real, stock_minimo, stock_maximo, stock_teorico,
            consumo_escandallo, semanas_stock, pendiente_recibir,
            nombre_proveedor/proveedor_habitual.
        output_dir: Directorio de salida (data/processed/).

    Dependencies:
        - Llamada por: run_jobs.py (paso 6)
        - Escribe: data/processed/historico_stock.csv
        - Consumido por: dashboard/pages/3_Evolucion.py (gráficos temporales,
          proyección, evolución cobertura)
    """
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

    if not df_hist.empty:
        df_hist = df_hist[~((df_hist["fecha"] == fecha) & (df_hist["articulo"].isin(df_new["articulo"])))]

    df_hist = pd.concat([df_hist, df_new], ignore_index=True)
    df_hist.to_csv(filepath, index=False)
    logger.info(f"Histórico stock actualizado: {len(df_new)} registros ({filepath})")


def guardar_historico_alertas(df_alertas: pd.DataFrame, output_dir: Path) -> None:
    """Registra las alertas disparadas en el histórico.

    Añade una fila por cada alerta activa. La deduplicación elimina todas
    las alertas previas de la misma fecha y artículo antes de insertar
    las nuevas, permitiendo que los cambios de configuración de alertas
    se reflejen correctamente.

    Args:
        df_alertas: DataFrame de alertas de evaluar_alertas(). Se extraen:
            fecha, semana, articulo, alerta_id, nivel, valor, umbral.
        output_dir: Directorio de salida (data/processed/).

    Dependencies:
        - Llamada por: run_jobs.py (paso 6)
        - Escribe: data/processed/historico_alertas.csv
        - Consumido por: dashboard/pages/3_Evolucion.py (mapa de calor,
          ranking recurrencia), dashboard/pages/1_Alertas_Activas.py
    """
    filepath = output_dir / "historico_alertas.csv"
    df_hist = _get_or_create_csv(filepath, HISTORICO_ALERTAS_COLS)

    if df_alertas.empty:
        logger.info("Sin alertas para registrar en histórico.")
        return

    fecha = df_alertas["fecha"].iloc[0]

    df_new = df_alertas[["fecha", "semana", "articulo", "alerta_id", "nivel", "valor", "umbral"]].copy()
    df_new = df_new.astype(str)

    if not df_hist.empty:
        df_hist = df_hist[~((df_hist["fecha"] == fecha) & (df_hist["articulo"].isin(df_new["articulo"])))]

    df_hist = pd.concat([df_hist, df_new], ignore_index=True)
    df_hist.to_csv(filepath, index=False)
    logger.info(f"Histórico alertas actualizado: {len(df_new)} registros")


def guardar_historico_consumo(df_procesado: pd.DataFrame, output_dir: Path) -> None:
    """Registra el consumo real semanal de cada artículo en el histórico.

    Args:
        df_procesado: DataFrame procesado por transformar_datos(). Se extraen:
            fecha_actualizacion, semana_iso, articulo, consumo_real_semana,
            consumo_escandallo, desviacion_consumo.
        output_dir: Directorio de salida (data/processed/).

    Dependencies:
        - Llamada por: run_jobs.py (paso 6)
        - Escribe: data/processed/historico_consumo.csv
        - Consumido por: dashboard/pages/2_Detalle_Articulo.py (gráfico consumo),
          dashboard/pages/3_Evolucion.py (consumo real vs escandallo)
    """
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
    """Actualiza el histórico de pedidos detectando nuevos y recepciones.

    Lógica de estados:
    1. Para cada pedido activo (pendiente_recibir > 0, numero_pedido válido):
       - Si es nuevo (no existe en el histórico), se inserta como EN_TRANSITO
         con fecha_estimada_entrega = fecha_pedido + (semanas_entrega × 7 días).
       - Si ya existe, no se modifica.
    2. Para pedidos previamente EN_TRANSITO que ya no aparecen con pendiente > 0:
       - Se marca como RECIBIDO con fecha_recepcion_real = fecha de ejecución.
       - Si fecha_recepcion_real > fecha_estimada_entrega, el estado pasa a RETRASADO.

    Args:
        df_procesado: DataFrame procesado por transformar_datos(). Se extraen:
            numero_pedido, articulo, pendiente_recibir, fecha_ultimo_pedido,
            semanas_entrega_prov, nombre_proveedor/proveedor_habitual,
            fecha_actualizacion.
        output_dir: Directorio de salida (data/processed/).

    Dependencies:
        - Llamada por: run_jobs.py (paso 6)
        - Escribe: data/processed/historico_pedidos.csv
        - Consumido por: dashboard/pages/2_Detalle_Articulo.py (timeline pedidos),
          dashboard/pages/4_Proveedores.py (fiabilidad = % RECIBIDO a tiempo)
    """
    filepath = output_dir / "historico_pedidos.csv"
    df_hist = _get_or_create_csv(filepath, HISTORICO_PEDIDOS_COLS)

    fecha_hoy = df_procesado["fecha_actualizacion"].iloc[0].strftime("%Y-%m-%d")

    pedidos_actuales = set()
    for _, row in df_procesado.iterrows():
        num_pedido = str(row.get("numero_pedido", "")).strip()
        art = str(row.get("articulo", "")).strip()
        pnd = row.get("pendiente_recibir", 0)
        if num_pedido and num_pedido != "0" and num_pedido != "nan" and pnd > 0:
            pedidos_actuales.add((num_pedido, art))

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

    if not df_hist.empty:
        for idx, row in df_hist.iterrows():
            if row["estado"] == "EN_TRANSITO":
                key = (str(row["numero_pedido"]), str(row["articulo"]))
                if key not in pedidos_actuales:
                    df_hist.at[idx, "estado"] = "RECIBIDO"
                    df_hist.at[idx, "fecha_recepcion_real"] = fecha_hoy
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
    """Registra una notificación enviada (o fallida) en el log.

    Args:
        output_dir: Directorio de salida (data/processed/).
        canal: Canal usado ("EMAIL" | "WHATSAPP").
        destinatarios: Lista de direcciones/teléfonos destinatarios.
        asunto: Asunto del email o resumen del mensaje.
        n_criticas: Número de alertas críticas incluidas.
        n_riesgo: Número de alertas de riesgo incluidas.
        n_articulos: Número de artículos afectados.
        estado: Resultado del envío ("ENVIADO" | "ERROR").
        error: Detalle del error si estado es "ERROR".

    Dependencies:
        - Llamada por: run_jobs.py (_enviar_notificaciones)
        - Escribe: data/processed/log_notificaciones.csv
        - Consumido por: dashboard/pages/1_Alertas_Activas.py
          (historial de notificaciones)
    """
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
    """Elimina registros más antiguos que el periodo de retención configurado.

    Recorre cada almacén histórico y elimina las filas cuya fecha es
    anterior al límite calculado (fecha_actual - semanas_retención).
    Solo modifica archivos que existen y que tienen registros a eliminar.

    La columna de fecha varía por archivo:
    - "fecha" para historico_stock, historico_alertas, historico_consumo
    - "fecha_hora" para log_notificaciones
    - "fecha_registro" para historico_pedidos

    Args:
        output_dir: Directorio donde están los CSV (data/processed/).
        config: Diccionario de configuración. Se usa la sección 'retencion':
            - historico_stock: 52 (semanas)
            - historico_alertas: 52
            - historico_consumo: 52
            - historico_pedidos: 104
            - log_notificaciones: 52

    Dependencies:
        - Llamada por: run_jobs.py (paso 9, al final de cada ejecución)
        - Modifica: todos los CSV en data/processed/
    """
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