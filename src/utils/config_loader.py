"""
Carga y gestión de configuración desde settings.yaml y .env.
"""
import os
import shutil
from pathlib import Path
from typing import Any

import yaml
from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
CONFIG_PATH = PROJECT_ROOT / "config" / "settings.yaml"


def load_config(config_path: Path = CONFIG_PATH) -> dict:
    """Carga settings.yaml y sobreescribe credenciales desde .env."""
    load_dotenv(PROJECT_ROOT / ".env")

    with open(config_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    # Inyectar credenciales desde variables de entorno
    smtp = config.get("notifications", {}).get("smtp", {})
    smtp["username"] = os.getenv("SMTP_USERNAME", smtp.get("username", ""))
    smtp["password"] = os.getenv("SMTP_PASSWORD", smtp.get("password", ""))
    smtp["from_address"] = os.getenv("SMTP_FROM", smtp.get("from_address", ""))

    return config


def save_config(config: dict, config_path: Path = CONFIG_PATH) -> None:
    """Guarda la configuración en settings.yaml con backup previo."""
    backup_path = config_path.with_suffix(".yaml.bak")
    if config_path.exists():
        shutil.copy2(config_path, backup_path)

    # Limpiar credenciales antes de escribir (se mantienen en .env)
    config_to_save = _deep_copy_without_secrets(config)

    with open(config_path, "w", encoding="utf-8") as f:
        yaml.dump(
            config_to_save,
            f,
            default_flow_style=False,
            allow_unicode=True,
            sort_keys=False,
        )


def _deep_copy_without_secrets(config: dict) -> dict:
    """Copia profunda limpiando campos sensibles de SMTP."""
    import copy
    c = copy.deepcopy(config)
    smtp = c.get("notifications", {}).get("smtp", {})
    for key in ("username", "password", "from_address"):
        smtp[key] = ""
    return c


def get_param(config: dict, key: str, default: Any = None) -> Any:
    """Obtiene un parámetro operativo por nombre."""
    return config.get("params", {}).get(key, default)


def get_alert_config(config: dict, alert_id: str) -> dict:
    """Obtiene la configuración de una alerta específica."""
    return config.get("alerts", {}).get(alert_id, {})


def get_active_recipients(config: dict, level: str, channel: str) -> list:
    """Devuelve destinatarios activos para un nivel y canal dados."""
    recipients = config.get("notifications", {}).get("recipients", [])
    return [
        r for r in recipients
        if r.get("active", False)
        and level in r.get("levels", [])
        and channel in r.get("channels", [])
    ]


def is_page_enabled(config: dict, page_key: str) -> bool:
    """Comprueba si una página del dashboard está habilitada."""
    return config.get("pages", {}).get(page_key, {}).get("enabled", True)
