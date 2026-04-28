"""
Lógica de envío de notificaciones por email.

Module: src.utils.email_sender
Purpose: Envía emails HTML con el informe de alertas de stock vía SMTP.
    Compatible con Gmail (desarrollo, puerto 587/TLS) y servidor SMTP
    corporativo (producción, puerto y TLS configurables).
Input: Configuración SMTP (de load_config), listas de alertas
Output: Email HTML enviado a los destinatarios configurados
Config: config/settings.yaml (notifications.smtp) + .env (credenciales SMTP)
Used by: run_jobs.py (_enviar_notificaciones, paso 8)

Formato del email:
    - Asunto dinámico: incluye fecha, conteo de alertas por nivel
    - Cuerpo HTML: tabla coloreada por criticidad (rojo/ámbar/azul)
      con columnas: Alerta, Artículo, Denominación, Proveedor, Valor, Acción
    - Pie con nota de generación automática
"""
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Optional

from src.utils.logger import setup_logger

logger = setup_logger("email_sender")


def send_email(
    smtp_config: dict,
    to_addresses: list[str],
    subject: str,
    html_body: str,
    cc_addresses: Optional[list[str]] = None,
) -> bool:
    """Envía un email HTML vía SMTP.

    Soporta conexión con o sin TLS según la configuración. Si use_tls
    es True (default), usa STARTTLS en el puerto configurado (típicamente 587).
    Si es False, conecta directamente sin cifrado (típico en servidores
    corporativos internos en puerto 25).

    No envía el email si las credenciales no están configuradas o si
    la lista de destinatarios está vacía, registrando un warning en el log.

    Args:
        smtp_config: Diccionario con la configuración SMTP:
            - host (str): dirección del servidor (ej: "smtp.gmail.com")
            - port (int): puerto (ej: 587, 25, 465)
            - use_tls (bool): si usar STARTTLS
            - username (str): usuario para autenticación
            - password (str): contraseña del usuario
            - from_address (str): dirección del remitente
        to_addresses: Lista de direcciones email de destinatarios.
        subject: Asunto del email.
        html_body: Cuerpo del email en formato HTML.
        cc_addresses: Lista opcional de direcciones en copia.

    Returns:
        True si el email se envió correctamente.
        False si hubo error (credenciales vacías, error SMTP, etc.).

    Dependencies:
        - Llamada por: run_jobs.py (_enviar_notificaciones)
        - Configuración: viene de load_config().notifications.smtp
        - Resultado registrado en: src.utils.persistence.registrar_notificacion()
    """
    if not smtp_config.get("username") or not smtp_config.get("password"):
        logger.warning("Credenciales SMTP no configuradas. Email no enviado.")
        return False

    if not to_addresses:
        logger.warning("Sin destinatarios. Email no enviado.")
        return False

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = smtp_config.get("from_address", smtp_config["username"])
    msg["To"] = ", ".join(to_addresses)
    if cc_addresses:
        msg["Cc"] = ", ".join(cc_addresses)

    msg.attach(MIMEText(html_body, "html", "utf-8"))

    all_recipients = to_addresses + (cc_addresses or [])

    try:
        if smtp_config.get("use_tls", True):
            server = smtplib.SMTP(smtp_config["host"], smtp_config["port"])
            server.ehlo()
            server.starttls()
            server.ehlo()
        else:
            server = smtplib.SMTP(smtp_config["host"], smtp_config["port"])

        server.login(smtp_config["username"], smtp_config["password"])
        server.sendmail(msg["From"], all_recipients, msg.as_string())
        server.quit()

        logger.info(f"Email enviado a {all_recipients}: {subject}")
        return True

    except smtplib.SMTPException as e:
        logger.error(f"Error SMTP al enviar email: {e}")
        return False
    except Exception as e:
        logger.error(f"Error inesperado al enviar email: {e}")
        return False


def build_alert_email_html(
    alertas_criticas: list[dict],
    alertas_riesgo: list[dict],
    alertas_info: list[dict],
    fecha_actualizacion: str,
) -> tuple[str, str]:
    """Construye el asunto y cuerpo HTML del email de alertas de stock.

    Genera un email con tabla HTML coloreada por nivel de criticidad:
    - Filas rojas (#FFCCCC) para alertas críticas
    - Filas ámbar (#FFF3CD) para alertas de riesgo
    - Filas azules (#CCE5FF) para alertas informativas

    Cada fila muestra: tipo de alerta, artículo, denominación, proveedor,
    valor que disparó la alerta, y acción recomendada.

    Args:
        alertas_criticas: Lista de diccionarios de alertas nivel CRITICA.
            Cada dict debe tener: alerta_id, nombre, articulo, denominacion,
            proveedor, valor, action.
        alertas_riesgo: Lista de diccionarios de alertas nivel RIESGO.
        alertas_info: Lista de diccionarios de alertas nivel INFORMATIVA.
        fecha_actualizacion: Fecha de actualización del ERP (formato string,
            ej: "2026-03-30"). Se muestra en el asunto y cabecera del email.

    Returns:
        Tupla (subject, html_body):
        - subject (str): asunto del email con conteo de alertas por nivel
        - html_body (str): cuerpo HTML completo del email

    Dependencies:
        - Llamada por: run_jobs.py (_enviar_notificaciones)
        - Usa internamente: _alert_row()
        - Los datos de entrada vienen de: evaluar_alertas() filtrado por
          notification_enabled
    """
    n_crit = len(alertas_criticas)
    n_risk = len(alertas_riesgo)
    n_info = len(alertas_info)
    total = n_crit + n_risk + n_info

    subject = f"🚨 Alertas Stock [{fecha_actualizacion}] — {n_crit} críticas, {n_risk} riesgo, {n_info} informativas"

    rows_html = ""
    for a in alertas_criticas:
        rows_html += _alert_row(a, "#FFCCCC", "#CC0000")
    for a in alertas_riesgo:
        rows_html += _alert_row(a, "#FFF3CD", "#886600")
    for a in alertas_info:
        rows_html += _alert_row(a, "#CCE5FF", "#004085")

    html_body = f"""
    <html>
    <body style="font-family: Arial, sans-serif; color: #333;">
      <h2 style="color: #1F3864;">Informe de Alertas de Stock</h2>
      <p>Fecha de actualización: <strong>{fecha_actualizacion}</strong></p>
      <p>Total de alertas: <strong>{total}</strong>
        (🔴 {n_crit} críticas, 🟡 {n_risk} riesgo, 🔵 {n_info} informativas)</p>

      <table style="border-collapse: collapse; width: 100%; font-size: 13px;">
        <tr style="background: #1F3864; color: white;">
          <th style="padding: 8px; text-align: left;">Alerta</th>
          <th style="padding: 8px; text-align: left;">Artículo</th>
          <th style="padding: 8px; text-align: left;">Denominación</th>
          <th style="padding: 8px; text-align: left;">Proveedor</th>
          <th style="padding: 8px; text-align: left;">Valor</th>
          <th style="padding: 8px; text-align: left;">Acción</th>
        </tr>
        {rows_html}
      </table>

      <p style="margin-top: 20px; font-size: 12px; color: #666;">
        Este informe ha sido generado automáticamente por el sistema de reporting.
      </p>
    </body>
    </html>
    """
    return subject, html_body


def _alert_row(alerta: dict, bg_color: str, text_color: str) -> str:
    """Genera una fila HTML de la tabla de alertas del email.

    Args:
        alerta: Diccionario con los datos de la alerta. Campos usados:
            alerta_id, nombre, articulo, denominacion, proveedor, valor, action.
        bg_color: Color de fondo de la fila en formato hex (ej: "#FFCCCC").
        text_color: Color del texto de la primera columna (ej: "#CC0000").

    Returns:
        String HTML con la fila <tr> completa.

    Dependencies:
        - Llamada por: build_alert_email_html()
    """
    return f"""
    <tr style="background: {bg_color};">
      <td style="padding: 6px 8px; border-bottom: 1px solid #ddd; color: {text_color}; font-weight: bold;">
        {alerta.get('alerta_id', '')} — {alerta.get('nombre', '')}
      </td>
      <td style="padding: 6px 8px; border-bottom: 1px solid #ddd;">{alerta.get('articulo', '')}</td>
      <td style="padding: 6px 8px; border-bottom: 1px solid #ddd;">{alerta.get('denominacion', '')}</td>
      <td style="padding: 6px 8px; border-bottom: 1px solid #ddd;">{alerta.get('proveedor', '')}</td>
      <td style="padding: 6px 8px; border-bottom: 1px solid #ddd;">{alerta.get('valor', '')}</td>
      <td style="padding: 6px 8px; border-bottom: 1px solid #ddd;">{alerta.get('action', '')}</td>
    </tr>
    """