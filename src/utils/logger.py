"""
Configuración centralizada de logging.

Module: src.utils.logger
Purpose: Proporciona un logger configurado con salida dual: consola (INFO+)
    y archivo diario (DEBUG+). Garantiza que toda ejecución automática
    deje un rastro completo en disco para depuración.
Output: Archivo logs/reporting_YYYYMMDD.log (un archivo por día)
Used by: Todos los módulos del proyecto vía setup_logger()

Formato de log:
    2026-03-30 08:15:23 | INFO     | etl_existencias | Datos extraídos: 29 artículos
    2026-03-30 08:15:24 | ERROR    | email_sender | Error SMTP al enviar email: ...
"""
import logging
import os
from datetime import datetime


def setup_logger(name: str = "reporting", log_dir: str = "logs") -> logging.Logger:
    """Configura y devuelve un logger con salida a consola y archivo diario.

    Crea el directorio de logs si no existe. Si el logger ya tiene
    handlers configurados (llamada repetida con el mismo nombre),
    devuelve el logger existente sin duplicar handlers.

    Niveles de log:
    - Archivo: DEBUG (registra todo, incluido detalle de operaciones)
    - Consola: INFO (solo mensajes relevantes para el operador)

    Args:
        name: Nombre del logger. Cada módulo usa un nombre único
            para identificar el origen del mensaje:
            - "run_jobs": orquestador principal
            - "etl_existencias": proceso ETL
            - "import_export": volcado de export.xlsx
            - "alerts_engine": motor de reglas de alerta
            - "email_sender": envío de emails
            - "persistence": escritura de históricos CSV
            - "reporting": nombre por defecto
        log_dir: Directorio donde se crean los archivos de log.
            Default: "logs" (relativo al directorio de trabajo).

    Returns:
        Logger configurado con dos handlers (archivo + consola).
        El mismo logger se reutiliza en llamadas posteriores con
        el mismo nombre.

    Dependencies:
        - Llamada por: todos los módulos al inicio (nivel de módulo)
        - Genera: logs/reporting_YYYYMMDD.log
    """
    os.makedirs(log_dir, exist_ok=True)

    logger = logging.getLogger(name)
    if logger.handlers:
        return logger

    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Archivo de log diario
    log_file = os.path.join(
        log_dir, f"reporting_{datetime.now().strftime('%Y%m%d')}.log"
    )
    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    # Consola
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    return logger