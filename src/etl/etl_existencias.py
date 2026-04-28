"""
ETL para el módulo de Existencias.
Lee el Excel del ERP, cruza con tablas maestras y genera el dataframe procesado.
"""
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta

from src.utils.logger import setup_logger

logger = setup_logger("etl_existencias")


def extraer_datos_erp(filepath: Path) -> dict:
    """
    Lee todas las hojas relevantes del Excel del ERP.

    Returns:
        dict con DataFrames: hoja_existencias, consumo_semanal, existencias_dia,
                             consumo_anual, proveedores, fecha_actualizacion
    """
    logger.info(f"Leyendo Excel: {filepath}")
    wb = pd.ExcelFile(filepath, engine="openpyxl")

    # --- HOJA EXISTENCIAS (datos brutos) ---
    df_raw = pd.read_excel(
        wb, sheet_name="HOJA EXISTENCIAS", header=5, dtype=str
    )
    # Limpiar nombres de columnas
    df_raw.columns = [str(c).strip() for c in df_raw.columns]

    # Fecha de actualización (celda A2)
    df_fecha = pd.read_excel(
        wb, sheet_name="HOJA EXISTENCIAS", header=None, nrows=3
    )
    fecha_actualizacion = None
    val_a2 = df_fecha.iloc[1, 0]
    if pd.notna(val_a2):
        if isinstance(val_a2, (int, float)):
            fecha_actualizacion = datetime(1899, 12, 30) + timedelta(days=int(val_a2))
        elif isinstance(val_a2, datetime):
            fecha_actualizacion = val_a2
        elif isinstance(val_a2, str):
            try:
                fecha_actualizacion = pd.to_datetime(val_a2, dayfirst=True)
            except Exception:
                fecha_actualizacion = datetime.now()

    if fecha_actualizacion is None:
        fecha_actualizacion = datetime.now()
        logger.warning("No se pudo leer fecha de actualización. Usando fecha actual.")

    # --- CONSUMO SEMANAL (tabla maestra) ---
    df_consumo = pd.read_excel(
        wb, sheet_name="CONSUMO SEMANAL", header=0, dtype=str
    )
    df_consumo.columns = [str(c).strip() for c in df_consumo.columns]

    # --- CONSUMO ANUAL ---
    df_anual = pd.read_excel(
        wb, sheet_name="CONSUMO ANUAL", header=0, dtype=str
    )
    df_anual.columns = [str(c).strip() for c in df_anual.columns]

    # --- EXISTENCIAS (hoja de análisis - proveedores filas 57+) ---
    df_exist_full = pd.read_excel(
        wb, sheet_name="EXISTENCIAS", header=None, dtype=str
    )

    # Extraer tabla de proveedores (filas 57+ aprox, cols G-I = 6-8)
    proveedores = _extraer_proveedores(df_exist_full)

    wb.close()

    logger.info(
        f"Datos extraídos: {len(df_raw)} artículos, "
        f"fecha actualización: {fecha_actualizacion.strftime('%d/%m/%Y')}"
    )

    return {
        "hoja_existencias": df_raw,
        "consumo_semanal": df_consumo,
        "consumo_anual": df_anual,
        "proveedores": proveedores,
        "fecha_actualizacion": fecha_actualizacion,
    }


def _extraer_proveedores(df_full: pd.DataFrame) -> pd.DataFrame:
    """Extrae la tabla de proveedores de la hoja EXISTENCIAS (filas 57+)."""
    proveedores = []
    for idx in range(56, min(len(df_full), 200)):
        cod = df_full.iloc[idx, 6]  # Col G
        nombre = df_full.iloc[idx, 7]  # Col H
        sem_entrega = df_full.iloc[idx, 9]  # Col J
        if pd.notna(cod) and str(cod).strip():
            try:
                cod_str = str(cod).strip()
                # Verificar que es un código numérico válido
                float(cod_str)
                proveedores.append({
                    "codigo_erp": cod_str,
                    "nombre": str(nombre).strip() if pd.notna(nombre) else "",
                    "semanas_entrega": _safe_float(sem_entrega),
                })
            except (ValueError, TypeError):
                continue

    df_prov = pd.DataFrame(proveedores)
    if not df_prov.empty:
        df_prov["codigo_erp"] = df_prov["codigo_erp"].apply(
            lambda x: str(int(float(x))) if x else ""
        )
    return df_prov


def transformar_datos(datos: dict, config: dict) -> pd.DataFrame:
    """
    Aplica la lógica de transformación equivalente a la hoja EXISTENCIAS.
    Cruza datos del ERP con CONSUMO SEMANAL y proveedores.

    Returns:
        DataFrame procesado con todas las columnas calculadas
    """
    df = datos["hoja_existencias"].copy()
    df_consumo = datos["consumo_semanal"]
    proveedores = datos["proveedores"]
    fecha_act = datos["fecha_actualizacion"]
    params = config.get("params", {})

    # Limpiar filas vacías
    col_articulo = df.columns[1]  # Col B = Artículo
    df = df[df[col_articulo].notna() & (df[col_articulo].astype(str).str.strip() != "")]
    df = df.reset_index(drop=True)
    # Limpiar espacios en todas las columnas de texto
    for col in df.columns:
        if df[col].dtype == object:
            df[col] = df[col].apply(
                lambda x: str(x).strip() if pd.notna(x) else x
            )

    # Mapear columnas del ERP a nombres estándar
    col_map = {
        df.columns[0]: "fecha_ultimo_pedido_raw",
        df.columns[1]: "articulo",
        df.columns[2]: "denominacion",
        df.columns[3]: "consumo_real_semana",
        df.columns[4]: "dias_servir",
        df.columns[5]: "stock_maximo",
        df.columns[7]: "stock_minimo",
        df.columns[8]: "existencia_real",
        df.columns[9]: "bajo_minimo",
    }
    # Columnas adicionales según posición
    if len(df.columns) > 13:
        col_map[df.columns[13]] = "pendiente_recibir"
    if len(df.columns) > 14:
        col_map[df.columns[14]] = "numero_pedido"
    if len(df.columns) > 15:
        col_map[df.columns[15]] = "fecha_ultimo_pedido"
    if len(df.columns) > 16:
        col_map[df.columns[16]] = "stock_teorico"
    if len(df.columns) > 17:
        col_map[df.columns[17]] = "codigo_proveedor"
    if len(df.columns) > 18:
        col_map[df.columns[18]] = "pedido_ideal"
    if len(df.columns) > 19:
        col_map[df.columns[19]] = "pedido_minimo"

    df = df.rename(columns=col_map)

    # Convertir numéricas
    num_cols = [
        "consumo_real_semana", "stock_maximo", "stock_minimo",
        "existencia_real", "bajo_minimo", "pendiente_recibir",
        "stock_teorico", "pedido_ideal", "pedido_minimo",
    ]
    for col in num_cols:
        if col in df.columns:
            df[col] = df[col].apply(
                lambda x: str(x).replace(",", ".").replace(" ", "").strip()
                if pd.notna(x) and str(x).strip() != "" else "0"
            )
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    # Convertir fechas
    for col in ["fecha_ultimo_pedido", "fecha_ultimo_pedido_raw"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
            df[col] = df[col].apply(
                lambda x: datetime(1899, 12, 30) + timedelta(days=int(x))
                if pd.notna(x) and x > 0 else pd.NaT
            )

    # --- CRUCE CON CONSUMO SEMANAL ---
    # Buscar columna de código en consumo semanal (primera columna)
    col_cod_consumo = df_consumo.columns[0]
    col_ajuste = df_consumo.columns[9] if len(df_consumo.columns) > 9 else None  # Col J
    col_obs = df_consumo.columns[13] if len(df_consumo.columns) > 13 else None  # Col N
    col_prov_hab = df_consumo.columns[12] if len(df_consumo.columns) > 12 else None  # Col M
    col_sem_ent = df_consumo.columns[10] if len(df_consumo.columns) > 10 else None  # Col K
    col_stock_min_cs = df_consumo.columns[6] if len(df_consumo.columns) > 6 else None  # Col G
    col_stock_max_cs = df_consumo.columns[7] if len(df_consumo.columns) > 7 else None  # Col H

    consumo_map = {}
    for _, row in df_consumo.iterrows():
        cod = str(row[col_cod_consumo]).strip() if pd.notna(row[col_cod_consumo]) else ""
        if cod:
            consumo_map[cod] = {
                "consumo_escandallo": _safe_float(row[col_ajuste]) if col_ajuste else 0,
                "observaciones": str(row[col_obs]).strip() if col_obs and pd.notna(row[col_obs]) else "",
                "proveedor_habitual": str(row[col_prov_hab]).strip() if col_prov_hab and pd.notna(row[col_prov_hab]) else "",
                "semanas_entrega_prov": _safe_float(row[col_sem_ent]) if col_sem_ent else 0,
            }

    # Aplicar cruce
    df["consumo_escandallo"] = df["articulo"].map(
        lambda x: consumo_map.get(str(x).strip(), {}).get("consumo_escandallo", 0)
    )
    df["observaciones"] = df["articulo"].map(
        lambda x: consumo_map.get(str(x).strip(), {}).get("observaciones", "")
    )
    df["proveedor_habitual"] = df["articulo"].map(
        lambda x: consumo_map.get(str(x).strip(), {}).get("proveedor_habitual", "")
    )
    df["semanas_entrega_prov"] = df["articulo"].map(
        lambda x: consumo_map.get(str(x).strip(), {}).get("semanas_entrega_prov", 0)
    )
    df["en_tabla_maestra"] = df["articulo"].map(
        lambda x: str(x).strip() in consumo_map
    )

    # --- CRUCE CON PROVEEDORES ---
    prov_map = {}
    if not proveedores.empty:
        for _, row in proveedores.iterrows():
            prov_map[row["codigo_erp"]] = row["nombre"]

    if "codigo_proveedor" in df.columns:
        df["codigo_proveedor_str"] = df["codigo_proveedor"].apply(
            lambda x: str(int(float(x))) if pd.notna(x) and x != "" else ""
        )
        df["nombre_proveedor"] = df["codigo_proveedor_str"].map(
            lambda x: prov_map.get(x, "")
        )
        df["codigo_proveedor_interno"] = df["codigo_proveedor"].apply(
            lambda x: int(float(x)) - params.get("codigo_base_proveedor", 400000000)
            if pd.notna(x) and _safe_float(x) > 0 else 0
        )

    # --- COLUMNAS CALCULADAS ---
    # Col A: Semanas de stock
    df["semanas_stock"] = np.where(
        df["consumo_escandallo"] > 0,
        df["existencia_real"] / df["consumo_escandallo"],
        np.nan,
    )

    # Col B: Semana de entrega
    df["semana_entrega"] = df["fecha_ultimo_pedido"].apply(
        lambda x: (x - fecha_act).days / 7 if pd.notna(x) else np.nan
    )

    # Col E: Desviación consumo real vs escandallo
    df["desviacion_consumo"] = np.where(
        df["consumo_real_semana"] > 0,
        1 - (df["consumo_escandallo"] / df["consumo_real_semana"]),
        np.nan,
    )

    # Col W: Stock real + pendiente - mínimo
    df["stock_vs_minimo"] = df["stock_teorico"] - df["stock_minimo"]

    # Col X: Fecha agotamiento estimada
    df["fecha_agotamiento"] = df.apply(
        lambda row: fecha_act + timedelta(
            days=(row["stock_teorico"] / row["consumo_escandallo"]) * 7
        ) if row["consumo_escandallo"] > 0 else pd.NaT,
        axis=1,
    )

    # Fecha de actualización como columna
    df["fecha_actualizacion"] = fecha_act
    df["semana_iso"] = fecha_act.isocalendar()[1]

    logger.info(f"Transformación completada: {len(df)} artículos procesados")
    return df


def _safe_float(val) -> float:
    """Convierte un valor a float de forma segura."""
    if pd.isna(val) or val == "" or val is None:
        return 0.0
    try:
        return float(val)
    except (ValueError, TypeError):
        return 0.0
