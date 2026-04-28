"""
Orquestador principal del sistema de reporting.

Module: run_jobs (raíz del proyecto)
Purpose: Ejecuta el flujo completo de procesamiento de datos en secuencia:
    importación del export → ETL → evaluación de alertas → persistencia
    de históricos → envío de notificaciones → retención de datos.
    Cada paso se ejecuta en un bloque try/except independiente para que
    el fallo de un paso no impida la ejecución de los demás.
Input: data/raw/export.xlsx (opcional), data/raw/EXISTENCIAS_MINIMO.xlsx
Output: data/processed/ (Excel procesado + 5 archivos CSV históricos)
Config: config/settings.yaml + .env
Used by: cron (Linux) o Programador de Tareas (Windows) para ejecución
    automática. También puede ejecutarse manualmente.

Uso:
    python run_jobs.py
    python run_jobs.py --excel data/raw/EXISTENCIAS_MINIMO.xlsx

Flujo de ejecución (11 pasos):
    1. Cargar configuración (settings.yaml + .env)
    2. Comprobar si hay export.xlsx nuevo (comparación fecha interna A2)
    2b. Si hay export nuevo, volcar a HOJA EXISTENCIAS
    3. Extraer datos del ERP (todas las hojas relevantes)
    4. Transformar datos (cruces, columnas calculadas)
    5. Evaluar alertas (10 reglas A1–A10)
    6. Persistir datos históricos (4 archivos CSV)
    7. Guardar Excel procesado para el dashboard
    8. Enviar notificaciones por email (si hay alertas habilitadas)
    9. Aplicar retención de datos (limpieza de registros antiguos)
"""
import sys
import argparse
from pathlib import Path
from datetime import datetime

# Asegurar que el proyecto está en el path
PROJECT_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.utils.logger import setup_logger
from src.utils.config_loader import load_config, get_active_recipients
from src.utils.email_sender import send_email, build_alert_email_html
from src.utils.persistence import (
    guardar_historico_stock,
    guardar_historico_alertas,
    guardar_historico_consumo,
    actualizar_historico_pedidos,
    registrar_notificacion,
    aplicar_retencion,
)
from src.etl.etl_existencias import extraer_datos_erp, transformar_datos
from src.alerts.rules_existencias import evaluar_alertas
from src.etl.import_export import volcar_export, hay_export_nuevo

logger = setup_logger("run_jobs")


def main():
    """Punto de entrada principal del orquestador.

    Parsea argumentos de línea de comandos, carga la configuración y
    ejecuta los 9 pasos del flujo en secuencia. Cada paso tiene su
    propio bloque try/except para garantizar que un fallo individual
    no detiene el resto del proceso.

    El argumento --excel permite especificar un Excel alternativo,
    útil para reprocesar ficheros específicos sin tocar la configuración.
    Si no se especifica, usa la ruta definida en settings.yaml.

    Dependencies:
        - Carga: src.utils.config_loader.load_config()
        - Paso 1-2: src.etl.import_export (hay_export_nuevo, volcar_export)
        - Paso 3: src.etl.etl_existencias.extraer_datos_erp()
        - Paso 4: src.etl.etl_existencias.transformar_datos()
        - Paso 5: src.alerts.rules_existencias.evaluar_alertas()
        - Paso 6: src.utils.persistence (guardar_historico_*)
        - Paso 7: pandas.DataFrame.to_excel()
        - Paso 8: _enviar_notificaciones() → src.utils.email_sender
        - Paso 9: src.utils.persistence.aplicar_retencion()
    """
    parser = argparse.ArgumentParser(description="Orquestador de reporting")
    parser.add_argument(
        "--excel", type=str, default=None,
        help="Ruta al Excel del ERP (por defecto usa config/settings.yaml)"
    )
    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info("INICIO DE EJECUCIÓN")
    logger.info("=" * 60)

    # 1. Cargar configuración
    try:
        config = load_config()
        logger.info("Configuración cargada correctamente")
    except Exception as e:
        logger.error(f"Error cargando configuración: {e}")
        sys.exit(1)

    # 2. Determinar ruta del Excel
    if args.excel:
        excel_path = Path(args.excel)
    else:
        raw_dir = PROJECT_ROOT / config["paths"]["raw_data"]
        excel_path = raw_dir / config["paths"]["excel_filename"]
        # 2b. Volcar export.xlsx si existe uno nuevo
        export_path = raw_dir / config["paths"].get("export_filename", "export.xlsx")
        if hay_export_nuevo(export_path, excel_path):
            try:
                volcado_ok = volcar_export(export_path, excel_path)
                if volcado_ok:
                    logger.info("Export volcado correctamente a HOJA EXISTENCIAS")
                else:
                    logger.warning("No se pudo volcar el export. Se procesará el existente.")
            except Exception as e:
                logger.error(f"Error en volcado de export: {e}")
        elif export_path.exists():
            logger.info("Export.xlsx no es más reciente. Se procesa el existente.")
        else:
            logger.info("Sin export.xlsx. Se procesa EXISTENCIAS_MINIMO.xlsx directamente.")

    if not excel_path.exists():
        logger.error(f"Excel no encontrado: {excel_path}")
        sys.exit(1)

    output_dir = PROJECT_ROOT / config["paths"]["processed_data"]
    output_dir.mkdir(parents=True, exist_ok=True)

    # 3. Extraer datos del ERP
    try:
        datos = extraer_datos_erp(excel_path)
        logger.info("Extracción de datos completada")
    except Exception as e:
        logger.error(f"Error en extracción de datos: {e}")
        sys.exit(1)

    # 4. Transformar datos
    try:
        df_procesado = transformar_datos(datos, config)
        logger.info("Transformación de datos completada")
    except Exception as e:
        logger.error(f"Error en transformación: {e}")
        sys.exit(1)

    # 5. Evaluar alertas
    try:
        df_alertas = evaluar_alertas(df_procesado, config)
        logger.info("Evaluación de alertas completada")
    except Exception as e:
        logger.error(f"Error evaluando alertas: {e}")
        df_alertas = None

    # 6. Persistir datos históricos
    try:
        guardar_historico_stock(df_procesado, output_dir)
        if df_alertas is not None:
            guardar_historico_alertas(df_alertas, output_dir)
        guardar_historico_consumo(df_procesado, output_dir)
        actualizar_historico_pedidos(df_procesado, output_dir)
        logger.info("Datos históricos actualizados")
    except Exception as e:
        logger.error(f"Error persistiendo datos: {e}")

    # 7. Guardar Excel procesado para el dashboard
    try:
        df_procesado.to_excel(
            output_dir / "existencias_procesado.xlsx",
            index=False, engine="openpyxl"
        )
        logger.info("Excel procesado guardado para dashboard")
    except Exception as e:
        logger.error(f"Error guardando Excel procesado: {e}")

    # 8. Enviar notificaciones
    if df_alertas is not None and not df_alertas.empty:
        _enviar_notificaciones(df_alertas, config, output_dir)

    # 9. Aplicar retención de datos
    try:
        aplicar_retencion(output_dir, config)
    except Exception as e:
        logger.warning(f"Error aplicando retención: {e}")

    logger.info("=" * 60)
    logger.info("FIN DE EJECUCIÓN")
    logger.info("=" * 60)


def _enviar_notificaciones(df_alertas, config, output_dir):
    """Envía notificaciones por email según la configuración de destinatarios.

    Flujo de envío:
    1. Filtra las alertas que tienen notification_enabled=True.
    2. Si hay alertas críticas, envía un email completo (críticas + riesgo + info)
       a los destinatarios configurados para el nivel CRITICA.
    3. Si hay alertas de riesgo, envía un email (solo riesgo + info) a los
       destinatarios de RIESGO que NO hayan recibido ya el email de críticas.
    4. Cada envío se registra en log_notificaciones.csv con su resultado.

    Args:
        df_alertas: DataFrame de alertas de evaluar_alertas().
        config: Diccionario de configuración con notifications y alerts.
        output_dir: Directorio de salida para el log de notificaciones.

    Dependencies:
        - Llamada por: main() (paso 8, solo si hay alertas)
        - Usa: get_active_recipients(), build_alert_email_html(), send_email(),
          registrar_notificacion()
        - Lee: config.notifications.smtp, config.notifications.recipients
    """
    df_notif = df_alertas[df_alertas["notification_enabled"] == True]
    if df_notif.empty:
        logger.info("Sin alertas con notificación habilitada")
        return

    alertas_criticas = df_notif[df_notif["nivel"] == "CRITICA"].to_dict("records")
    alertas_riesgo = df_notif[df_notif["nivel"] == "RIESGO"].to_dict("records")
    alertas_info = df_notif[df_notif["nivel"] == "INFORMATIVA"].to_dict("records")

    fecha_act = df_alertas["fecha"].iloc[0]

    # Email para alertas críticas
    if alertas_criticas:
        recipients_crit = get_active_recipients(config, "CRITICA", "email")
        if recipients_crit:
            to_list = [r["email"] for r in recipients_crit if r.get("email")]
            subject, html = build_alert_email_html(
                alertas_criticas, alertas_riesgo, alertas_info, fecha_act
            )
            smtp_config = config.get("notifications", {}).get("smtp", {})
            success = send_email(smtp_config, to_list, subject, html)

            registrar_notificacion(
                output_dir, "EMAIL", to_list, subject,
                len(alertas_criticas), len(alertas_riesgo),
                len(set(a["articulo"] for a in alertas_criticas + alertas_riesgo)),
                "ENVIADO" if success else "ERROR",
                "" if success else "Error en envío SMTP"
            )

    # Email para alertas de riesgo
    if alertas_riesgo:
        recipients_risk = get_active_recipients(config, "RIESGO", "email")
        already_sent = set(r["email"] for r in get_active_recipients(config, "CRITICA", "email"))
        risk_only = [r for r in recipients_risk if r.get("email") not in already_sent]

        if risk_only:
            to_list = [r["email"] for r in risk_only if r.get("email")]
            subject, html = build_alert_email_html(
                [], alertas_riesgo, alertas_info, fecha_act
            )
            smtp_config = config.get("notifications", {}).get("smtp", {})
            success = send_email(smtp_config, to_list, subject, html)

            registrar_notificacion(
                output_dir, "EMAIL", to_list, subject,
                0, len(alertas_riesgo),
                len(set(a["articulo"] for a in alertas_riesgo)),
                "ENVIADO" if success else "ERROR",
            )

    logger.info("Proceso de notificaciones completado")


if __name__ == "__main__":
    main()