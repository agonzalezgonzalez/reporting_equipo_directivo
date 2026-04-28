"""
ETL para el módulo de Existencias.

Module: src.etl.etl_existencias
Purpose: Lee el fichero Excel del ERP (EXISTENCIAS_MINIMO.xlsx), extrae los
    datos de las hojas relevantes (HOJA EXISTENCIAS, CONSUMO SEMANAL,
    CONSUMO ANUAL, EXISTENCIAS), cruza la información entre hojas y
    genera un DataFrame procesado con todas las columnas calculadas
    listas para la evaluación de alertas y el dashboard.
Input: Fichero Excel EXISTENCIAS_MINIMO.xlsx en data/raw/
Output: DataFrame procesado con columnas originales del ERP + columnas
    calculadas (semanas_stock, desviacion_consumo, stock_vs_minimo,
    fecha_agotamiento, etc.)
Config: config/settings.yaml (sección params: codigo_base_proveedor)
Used by: run_jobs.py (paso 3-4), dashboard/app_main.py,
    dashboard/pages/1_Alertas_Activas.py,
    dashboard/pages/2_Detalle_Articulo.py,
    dashboard/pages/4_Proveedores.py
"""
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta

from src.utils.logger import setup_logger

logger = setup_logger("etl_existencias")


def extraer_datos_erp(filepath: Path) -> dict:
    """Lee todas las hojas relevantes del Excel del ERP y extrae los datos brutos.

    Abre el libro EXISTENCIAS_MINIMO.xlsx y extrae:
    - HOJA EXISTENCIAS: datos brutos de artículos bajo mínimos (header en fila 6).
    - Fecha de actualización: celda A2 de HOJA EXISTENCIAS (fecha de descarga del ERP).
    - CONSUMO SEMANAL: tabla maestra con consumo ajustado, proveedores, observaciones.
    - CONSUMO ANUAL: histórico de consumo anual por producto.
    - Tabla de proveedores: filas 57+ de la hoja EXISTENCIAS (códigos ERP → nombres).

    La fecha de actualización se intenta leer en tres formatos: numérico Excel
    (días desde 1899-12-30), datetime nativo, o string con formato dd/mm/yyyy.
    Si no se puede leer, se usa la fecha actual como fallback.

    Args:
        filepath: Ruta al fichero EXISTENCIAS_MINIMO.xlsx.
            Normalmente en data/raw/EXISTENCIAS_MINIMO.xlsx.

    Returns:
        Diccionario con las siguientes claves:
        - hoja_existencias (pd.DataFrame): Datos brutos del ERP, todas las columnas
          como string (dtype=str). Header en fila 6 (header=5 en pandas).
        - consumo_semanal (pd.DataFrame): Tabla maestra, header en fila 1.
        - consumo_anual (pd.DataFrame): Histórico anual, header en fila 1.
        - proveedores (pd.DataFrame): Tabla con columnas codigo_erp, nombre,
          semanas_entrega. Extraída de filas 57+ de la hoja EXISTENCIAS.
        - fecha_actualizacion (datetime): Fecha de la descarga del ERP.

    Raises:
        FileNotFoundError: Si el fichero no existe.
        ValueError: Si el Excel no contiene las hojas esperadas.

    Dependencies:
        - Llamada por: run_jobs.py (paso 3), dashboard (vía cargar_datos())
        - Usa internamente: _extraer_proveedores()
        - Su salida alimenta a: transformar_datos()
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
    """Extrae la tabla de proveedores de la hoja EXISTENCIAS (filas 57+).

    Recorre las filas 57 a 200 de la hoja EXISTENCIAS buscando códigos
    de proveedor numéricos en la columna G (índice 6). Para cada código
    válido, extrae el nombre (col H) y las semanas de entrega (col J).

    Los códigos no numéricos (cabeceras, texto, fórmulas) se descartan
    silenciosamente.

    Args:
        df_full: DataFrame completo de la hoja EXISTENCIAS leído sin header
            (header=None). Todas las columnas como string.

    Returns:
        DataFrame con columnas:
        - codigo_erp (str): Código del proveedor en el ERP (ej: "400000174").
          Normalizado a entero sin decimales.
        - nombre (str): Nombre del proveedor (ej: "GRAFIMOL").
        - semanas_entrega (float): Semanas de entrega del proveedor (ej: 4.0).

    Dependencies:
        - Llamada por: extraer_datos_erp()
        - Usa: _safe_float()
        - Sus datos se usan en: transformar_datos() para cruzar código
          proveedor del ERP → nombre legible (columna O de EXISTENCIAS)
    """
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
    """Aplica la lógica de transformación equivalente a la hoja EXISTENCIAS del Excel.

    Toma los datos brutos del ERP y los enriquece cruzándolos con las tablas
    maestras (CONSUMO SEMANAL y proveedores). Genera las columnas calculadas
    que replican las fórmulas de la hoja EXISTENCIAS del documento original:

    Columnas calculadas generadas:
    - semanas_stock (col A): existencia_real / consumo_escandallo
    - semana_entrega (col B): (fecha_ultimo_pedido - fecha_actualizacion) / 7
    - consumo_escandallo (col C): VLOOKUP desde CONSUMO SEMANAL col J
    - desviacion_consumo (col E): 1 - (consumo_escandallo / consumo_real_semana)
    - nombre_proveedor (col O): VLOOKUP desde tabla proveedores filas 57+
    - codigo_proveedor_interno (col P): codigo_proveedor - 400000000
    - stock_vs_minimo (col W): stock_teorico - stock_minimo
    - fecha_agotamiento (col X): fecha_act + (stock_teorico / consumo_escandallo) * 7

    El proceso incluye:
    1. Limpieza de filas vacías y espacios en blanco.
    2. Renombrado de columnas posicionales a nombres semánticos.
    3. Conversión de columnas numéricas (maneja comas como separador decimal).
    4. Conversión de fechas Excel (número serial → datetime).
    5. Cruce con CONSUMO SEMANAL por código de artículo.
    6. Cruce con tabla de proveedores por código ERP.
    7. Cálculo de columnas derivadas.

    Args:
        datos: Diccionario devuelto por extraer_datos_erp(). Debe contener:
            - hoja_existencias: DataFrame con datos brutos del ERP.
            - consumo_semanal: DataFrame de la tabla maestra.
            - proveedores: DataFrame de la tabla de proveedores.
            - fecha_actualizacion: datetime con la fecha base del ERP.
        config: Diccionario de configuración cargado por load_config().
            Se usa la clave params.codigo_base_proveedor (default: 400000000)
            para calcular el código interno del proveedor.

    Returns:
        DataFrame con todas las columnas originales renombradas más las
        columnas calculadas. Incluye también:
        - fecha_actualizacion (datetime): fecha base para cálculos.
        - semana_iso (int): número de semana ISO del año.
        - en_tabla_maestra (bool): si el artículo existe en CONSUMO SEMANAL.
        - observaciones (str): notas de la col N de CONSUMO SEMANAL.
        - proveedor_habitual (str): nombre del proveedor de CONSUMO SEMANAL col M.
        - semanas_entrega_prov (float): semanas de entrega de CONSUMO SEMANAL col K.

    Dependencies:
        - Consume la salida de: extraer_datos_erp()
        - Configuración: config/settings.yaml (params.codigo_base_proveedor)
        - Consumido por: src.alerts.rules_existencias.evaluar_alertas(),
          run_jobs.py (paso 5), todas las páginas del dashboard
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
    """Convierte un valor a float de forma segura, devolviendo 0.0 ante cualquier error.

    Maneja los casos habituales de datos del ERP: valores None, NaN, strings
    vacíos, y strings no numéricos.

    Args:
        val: Valor a convertir. Puede ser int, float, str, None o NaN.

    Returns:
        El valor como float, o 0.0 si la conversión falla.

    Dependencies:
        - Usada por: _extraer_proveedores(), transformar_datos()
          (cruce con CONSUMO SEMANAL)
    """
    if pd.isna(val) or val == "" or val is None:
        return 0.0
    try:
        return float(val)
    except (ValueError, TypeError):
        return 0.0