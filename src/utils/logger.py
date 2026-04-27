"""
Configuración centralizada de logging.
Toda ejecución automática deja rastro en archivo .log.
"""
import logging
import os
from datetime import datetime


def setup_logger(name: str = "reporting", log_dir: str = "logs") -> logging.Logger:
    """Configura y devuelve un logger con salida a consola y archivo."""
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
