"""
============================================================================
Alerting Hook - Dagster ETL (Enhanced)
============================================================================
Hook pour envoyer des alertes Teams/Email sur échec/succès de run
"""

import logging
import os
from datetime import datetime
from typing import Optional

import requests
from dagster import HookContext, failure_hook, success_hook

logger = logging.getLogger(__name__)


def send_teams_notification(
    webhook_url: str,
    title: str,
    message: str,
    color: str = "0078D4",
    facts: Optional[dict] = None,
    action_url: Optional[str] = None,
) -> bool:
    """
    Envoie notification Teams via webhook (format MessageCard)
    
    Args:
        webhook_url: URL du webhook Teams
        title: Titre de la notification
        message: Message principal
        color: Couleur (hex sans #)
        facts: Dict de faits additionnels
        action_url: URL pour bouton d'action
    
    Returns:
        True si succès
    """
    card = {
        "@type": "MessageCard",
        "@context": "https://schema.org/extensions",
        "summary": title,
        "themeColor": color,
        "title": title,
        "text": message,
    }
    
    if facts:
        card["sections"] = [{
            "activityTitle": "Détails du Run",
            "facts": [{"name": k, "value": str(v)} for k, v in facts.items()]
        }]
    
    if action_url:
        card["potentialAction"] = [{
            "@type": "OpenUri",
            "name": "🔍 Voir dans Dagster",
            "targets": [{"os": "default", "uri": action_url}]
        }]
    
    try:
        response = requests.post(
            webhook_url,
            json=card,
            timeout=10,
        )
        response.raise_for_status()
        logger.info(f"[TEAMS] Notification envoyée: {title}")
        return True
        
    except Exception as e:
        logger.error(f"[TEAMS ERROR] {e}")
        return False


def send_email_notification(
    smtp_host: str,
    smtp_port: int,
    from_email: str,
    to_email: str,
    subject: str,
    body: str,
    smtp_user: Optional[str] = None,
    smtp_password: Optional[str] = None,
) -> bool:
    """
    Envoie notification email
    
    Args:
        smtp_host: Serveur SMTP
        smtp_port: Port SMTP
        from_email: Email expéditeur
        to_email: Email destinataire
        subject: Sujet
        body: Corps du message
        smtp_user: User SMTP (optionnel)
        smtp_password: Password SMTP (optionnel)
    
    Returns:
        True si succès
    """
    import smtplib
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText
    
    try:
        msg = MIMEMultipart()
        msg["From"] = from_email
        msg["To"] = to_email
        msg["Subject"] = subject
        msg.attach(MIMEText(body, "html"))
        
        with smtplib.SMTP(smtp_host, smtp_port, timeout=10) as server:
            if smtp_user and smtp_password:
                server.starttls()
                server.login(smtp_user, smtp_password)
            
            server.send_message(msg)
        
        logger.info(f"[EMAIL] Notification envoyée: {subject}")
        return True
        
    except Exception as e:
        logger.error(f"[EMAIL ERROR] {e}")
        return False


@failure_hook
def alert_on_failure(context: HookContext):
    """
    Hook Dagster déclenché sur échec d'un run (Enhanced)
    """
    run_id = context.run_id
    job_name = context.job_name
    step_key = context.step_key
    
    # Configuration depuis env
    teams_webhook = os.getenv("TEAMS_WEBHOOK_URL")
    email_enabled = os.getenv("EMAIL_ALERTS_ENABLED", "false").lower() == "true"
    dagster_url = os.getenv("DAGSTER_URL", "http://172.30.27.14:3000")
    
    # Construire URL Dagster pour ce run
    run_url = f"{dagster_url}/runs/{run_id}"
    
    # Timestamp formaté
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    title = f"❌ ETL Pipeline Failed"
    message = f"**Job:** {job_name}\n**Step:** `{step_key}`\n**Time:** {timestamp}"
    
    facts = {
        "🔧 Job": job_name,
        "📦 Step": step_key,
        "🆔 Run ID": run_id[:8] + "...",  # Short ID
        "🕐 Time": timestamp,
        "💻 Server": "S918-ETL-01-FR",
        "🚨 Status": "FAILED"
    }
    
    # Teams notification
    if teams_webhook:
        send_teams_notification(
            webhook_url=teams_webhook,
            title=title,
            message=message,
            color="FF0000",  # Rouge
            facts=facts,
            action_url=run_url,
        )
    
    # Email notification
    if email_enabled:
        smtp_host = os.getenv("SMTP_SERVER", "localhost")
        smtp_port = int(os.getenv("SMTP_PORT", "25"))
        from_email = os.getenv("ALERT_FROM_EMAIL", "etl@cbm.local")
        to_email = os.getenv("ALERT_TO_EMAILS", "data-team@cbm.local")
        
        email_body = f"""
        <html>
        <body style="font-family: Arial, sans-serif;">
            <h2 style="color: #d32f2f;">❌ ETL Pipeline Failed</h2>
            <table style="border-collapse: collapse; width: 100%;">
                <tr><td style="padding: 8px; background: #f5f5f5;"><b>Job</b></td><td style="padding: 8px;">{job_name}</td></tr>
                <tr><td style="padding: 8px; background: #f5f5f5;"><b>Step</b></td><td style="padding: 8px;">{step_key}</td></tr>
                <tr><td style="padding: 8px; background: #f5f5f5;"><b>Run ID</b></td><td style="padding: 8px;">{run_id}</td></tr>
                <tr><td style="padding: 8px; background: #f5f5f5;"><b>Time</b></td><td style="padding: 8px;">{timestamp}</td></tr>
            </table>
            <p><a href="{run_url}" style="background: #1976d2; color: white; padding: 10px 20px; text-decoration: none; display: inline-block; margin-top: 10px;">View in Dagster</a></p>
        </body>
        </html>
        """
        
        send_email_notification(
            smtp_host=smtp_host,
            smtp_port=smtp_port,
            from_email=from_email,
            to_email=to_email,
            subject=f"❌ ETL Failed: {job_name}",
            body=email_body,
        )


@success_hook
def alert_on_success(context: HookContext):
    """
    Hook Dagster déclenché sur succès d'un run (Enhanced)
    """
    # Optionnel: notification uniquement pour pipeline complet
    job_name = context.job_name
    
    # Ne notifier que pour le job principal
    if job_name != "full_etl_pipeline":
        return
    
    run_id = context.run_id
    teams_webhook = os.getenv("TEAMS_WEBHOOK_URL")
    dagster_url = os.getenv("DAGSTER_URL", "http://172.30.27.14:3000")
    
    run_url = f"{dagster_url}/runs/{run_id}"
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    if teams_webhook:
        title = f"✅ ETL Pipeline Succeeded"
        message = f"**Job:** {job_name}\n**Time:** {timestamp}"
        
        send_teams_notification(
            webhook_url=teams_webhook,
            title=title,
            message=message,
            color="28A745",  # Vert
            facts={
                "🔧 Job": job_name,
                "🆔 Run ID": run_id[:8] + "...",
                "🕐 Time": timestamp,
                "💻 Server": "S918-ETL-01-FR",
                "✅ Status": "SUCCESS"
            },
            action_url=run_url,
        )


# Export hooks
__all__ = [
    "alert_on_failure",
    "alert_on_success",
    "send_teams_notification",
    "send_email_notification",
]