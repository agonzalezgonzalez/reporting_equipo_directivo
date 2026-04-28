"""
Lógica de envío de notificaciones por email.
Compatible con Gmail (desarrollo) y SMTP corporativo (producción).
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
    """
    Envía un email HTML vía SMTP.

    Args:
        smtp_config: dict con host, port, use_tls, username, password, from_address
        to_addresses: lista de destinatarios
        subject: asunto del email
        html_body: cuerpo en HTML
        cc_addresses: lista de copia (opcional)

    Returns:
        True si se envió correctamente, False si hubo error
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
    """
    Construye el asunto y cuerpo HTML del email de alertas.

    Returns:
        (subject, html_body)
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
