"""
Carga y gestión de configuración desde settings.yaml y .env.

Module: src.utils.config_loader
Purpose: Centraliza la carga de configuración del proyecto. Combina el archivo
    settings.yaml (configuración base y parámetros operativos) con las
    variables de entorno del archivo .env (credenciales y configuración
    específica de entorno). Garantiza que los secretos nunca se escriban
    en el archivo YAML.
Input: config/settings.yaml + .env (raíz del proyecto)
Output: Diccionario de configuración unificado
Config: Se autoconfigura leyendo sus propios ficheros
Used by: Todos los módulos del proyecto que necesitan configuración:
    run_jobs.py, src/etl/*, src/alerts/*, src/utils/email_sender.py,
    src/utils/persistence.py, todas las páginas del dashboard

Modelo de doble gestión:
    - settings.yaml: fuente de verdad persistente. Contiene parámetros
      operativos, configuración de alertas, destinatarios y páginas.
      Editable manualmente o desde el panel de Administración (Streamlit).
    - .env: credenciales y configuración específica de entorno (SMTP, Twilio).
      Nunca se versiona con Git. Diferente en cada entorno (dev/prod).

Prioridad: Las variables de .env sobreescriben los valores de settings.yaml.
    Si una variable no existe en .env, se usa el valor de settings.yaml como
    fallback.
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
    """Carga la configuración unificada desde settings.yaml y .env.

    Lee settings.yaml como base y sobreescribe los campos de SMTP con
    las variables de entorno de .env si existen. Esto permite tener
    un settings.yaml idéntico en todos los entornos y diferenciar
    solo con el .env.

    Variables de entorno leídas:
    - ENV: entorno actual ("development" | "production")
    - SMTP_HOST: servidor de correo
    - SMTP_PORT: puerto SMTP
    - SMTP_USE_TLS: si usa TLS ("true" | "false")
    - SMTP_USERNAME: usuario SMTP
    - SMTP_PASSWORD: contraseña SMTP
    - SMTP_FROM: dirección del remitente

    Args:
        config_path: Ruta al archivo settings.yaml.
            Default: config/settings.yaml relativo a la raíz del proyecto.

    Returns:
        Diccionario con toda la configuración. Incluye una clave interna
        '_env' con el entorno actual (no se persiste al guardar).
        Estructura principal:
        - paths: rutas de datos (raw_data, processed_data, excel_filename, export_filename)
        - params: parámetros operativos (umbrales, rappels)
        - retencion: semanas de retención por almacén histórico
        - alerts: configuración por alerta (A1–A10)
        - notifications: SMTP, WhatsApp y lista de destinatarios
        - pages: visibilidad de páginas del dashboard
        - _env: "development" | "production" (metadato interno)

    Raises:
        FileNotFoundError: Si settings.yaml no existe.
        yaml.YAMLError: Si settings.yaml tiene errores de sintaxis.

    Dependencies:
        - Consumido por: todos los módulos del proyecto
        - Ficheros: config/settings.yaml, .env
        - Relacionado: save_config() para persistir cambios desde el dashboard
    """
    load_dotenv(PROJECT_ROOT / ".env")

    with open(config_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    # Inyectar configuración SMTP desde variables de entorno
    smtp = config.get("notifications", {}).get("smtp", {})
    smtp["host"] = os.getenv("SMTP_HOST", smtp.get("host", ""))
    smtp["port"] = int(os.getenv("SMTP_PORT", smtp.get("port", 587)))
    smtp["use_tls"] = os.getenv("SMTP_USE_TLS", str(smtp.get("use_tls", True))).lower() == "true"
    smtp["username"] = os.getenv("SMTP_USERNAME", smtp.get("username", ""))
    smtp["password"] = os.getenv("SMTP_PASSWORD", smtp.get("password", ""))
    smtp["from_address"] = os.getenv("SMTP_FROM", smtp.get("from_address", ""))

    config["_env"] = os.getenv("ENV", "development")

    return config


def save_config(config: dict, config_path: Path = CONFIG_PATH) -> None:
    """Guarda la configuración en settings.yaml con backup automático previo.

    Antes de escribir, crea una copia del archivo actual como
    settings.yaml.bak. Los campos sensibles (credenciales SMTP) se
    limpian antes de escribir porque viven exclusivamente en .env.
    El metadato '_env' también se elimina del archivo guardado.

    Args:
        config: Diccionario de configuración a guardar.
        config_path: Ruta destino del archivo YAML.

    Dependencies:
        - Llamada por: dashboard/pages/5_Admin.py (botón "Guardar configuración")
        - Usa: _deep_copy_without_secrets()
        - Genera: config/settings.yaml (actualizado), config/settings.yaml.bak (backup)
    """
    backup_path = config_path.with_suffix(".yaml.bak")
    if config_path.exists():
        shutil.copy2(config_path, backup_path)

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
    """Copia profunda del config limpiando campos que no deben persistirse en YAML.

    Elimina:
    - Credenciales SMTP (username, password, from_address) → viven en .env
    - Metadato '_env' → se genera dinámicamente en load_config()

    Args:
        config: Diccionario de configuración original.

    Returns:
        Copia profunda sin campos sensibles ni metadatos internos.

    Dependencies:
        - Llamada por: save_config()
    """
    import copy
    c = copy.deepcopy(config)
    smtp = c.get("notifications", {}).get("smtp", {})
    for key in ("username", "password", "from_address"):
        smtp[key] = ""
    c.pop("_env", None)
    return c


def get_param(config: dict, key: str, default: Any = None) -> Any:
    """Obtiene un parámetro operativo por nombre desde la sección 'params'.

    Args:
        config: Diccionario de configuración.
        key: Nombre del parámetro (ej: "ventana_riesgo_dias").
        default: Valor por defecto si no existe.

    Returns:
        Valor del parámetro, o default si no existe.

    Dependencies:
        - Parámetros disponibles: ventana_riesgo_dias, umbral_agotamiento_dias,
          umbral_cobertura_larga_dias, umbral_semanas_stock_critico,
          umbral_desviacion_consumo, rappel_saeco, rappel_xuber,
          codigo_base_proveedor
    """
    return config.get("params", {}).get(key, default)


def get_alert_config(config: dict, alert_id: str) -> dict:
    """Obtiene la configuración completa de una alerta específica.

    Args:
        config: Diccionario de configuración.
        alert_id: Identificador de la alerta (ej: "A1", "A10").

    Returns:
        Diccionario con: nombre, nivel, dashboard_enabled,
        notification_enabled, channels, action.
        Diccionario vacío si la alerta no está en la configuración.

    Dependencies:
        - Lee: config.alerts.{alert_id}
    """
    return config.get("alerts", {}).get(alert_id, {})


def get_active_recipients(config: dict, level: str, channel: str) -> list:
    """Devuelve la lista de destinatarios activos para un nivel de alerta y canal.

    Filtra la lista de destinatarios por tres criterios simultáneos:
    - active == True (no desactivado temporalmente)
    - level está en la lista de levels del destinatario
    - channel está en la lista de channels del destinatario

    Args:
        config: Diccionario de configuración.
        level: Nivel de alerta a filtrar ("CRITICA" | "RIESGO" | "INFORMATIVA").
        channel: Canal de notificación ("email" | "whatsapp").

    Returns:
        Lista de diccionarios de destinatarios que cumplen los tres criterios.
        Cada diccionario contiene: name, email, phone, levels, channels, active.

    Dependencies:
        - Llamada por: run_jobs.py (_enviar_notificaciones)
        - Lee: config.notifications.recipients

    Example:
        >>> recipients = get_active_recipients(config, "CRITICA", "email")
        >>> emails = [r["email"] for r in recipients]
    """
    recipients = config.get("notifications", {}).get("recipients", [])
    return [
        r for r in recipients
        if r.get("active", False)
        and level in r.get("levels", [])
        and channel in r.get("channels", [])
    ]


def is_page_enabled(config: dict, page_key: str) -> bool:
    """Comprueba si una página del dashboard está habilitada.

    Args:
        config: Diccionario de configuración.
        page_key: Clave de la página en settings.yaml
            (ej: "alertas_activas", "proveedores").

    Returns:
        True si la página está habilitada o si no está en la configuración
        (habilitada por defecto).

    Dependencies:
        - Llamada por: cada página del dashboard al inicio
        - Lee: config.pages.{page_key}.enabled
    """
    return config.get("pages", {}).get(page_key, {}).get("enabled", True)


def get_env(config: dict) -> str:
    """Devuelve el entorno actual del sistema.

    Args:
        config: Diccionario de configuración (con _env inyectado por load_config).

    Returns:
        "development" o "production". Default: "development".

    Dependencies:
        - El valor se establece en load_config() desde la variable ENV de .env
    """
    return config.get("_env", "development")