"""
============================================================================
Alerting Hooks - Notifications Teams/Email (Enhanced)
============================================================================
"""

import logging
import os
from datetime import datetime
from typing import Optional

import requests
from dagster import HookContext, failure_hook, success_hook
import psycopg2

from src.config.settings import get_settings

logger = logging.getLogger(__name__)


# =============================================================================
# Helpers - Notifications
# =============================================================================

def send_teams_notification(
    webhook_url: str,
    title: str,
    message: str,
    color: str = "0078D4",
    facts: Optional[dict] = None,
    action_url: Optional[str] = None,
) -> bool:
    """
    Envoie notification Teams via webhook (format MessageCard).
    
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
    Envoie notification email.
    
    Args:
        smtp_host: Serveur SMTP
        smtp_port: Port SMTP
        from_email: Email expéditeur
        to_email: Email destinataire
        subject: Sujet
        body: Corps du message (HTML)
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


# =============================================================================
# Hooks Dagster
# =============================================================================

@failure_hook
def alert_on_failure(context: HookContext):
    """
    Hook Dagster déclenché sur échec d'un run.
    
    Envoie notifications Teams + Email selon configuration.
    """
    settings = get_settings()
    
    run_id = context.run_id
    job_name = context.job_name
    step_key = context.step_key
    
    # Construire URL Dagster
    dagster_url = os.getenv("DAGSTER_URL", "http://172.30.27.14:3000")
    run_url = f"{dagster_url}/runs/{run_id}"
    
    # Timestamp
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Message Teams
    title = "❌ ETL Pipeline Failed"
    message = f"**Job:** {job_name}\n**Step:** `{step_key}`\n**Time:** {timestamp}"
    
    facts = {
        "🔧 Job": job_name,
        "📦 Step": step_key,
        "🆔 Run ID": run_id[:8] + "...",
        "🕐 Time": timestamp,
        "💻 Server": "S918-ETL-02-FR",
        "🚨 Status": "FAILED"
    }
    
    # Teams notification
    if settings.teams_webhook_url:
        send_teams_notification(
            webhook_url=settings.teams_webhook_url,
            title=title,
            message=message,
            color="FF0000",  # Rouge
            facts=facts,
            action_url=run_url,
        )
    
    # Email notification
    if settings.email_alerts_enabled:
        email_body = f"""
        <html>
        <body style="font-family: Arial, sans-serif;">
            <h2 style="color: #d32f2f;">❌ ETL Pipeline Failed</h2>
            <table style="border-collapse: collapse; width: 100%;">
                <tr>
                    <td style="padding: 8px; background: #f5f5f5;"><b>Job</b></td>
                    <td style="padding: 8px;">{job_name}</td>
                </tr>
                <tr>
                    <td style="padding: 8px; background: #f5f5f5;"><b>Step</b></td>
                    <td style="padding: 8px;">{step_key}</td>
                </tr>
                <tr>
                    <td style="padding: 8px; background: #f5f5f5;"><b>Run ID</b></td>
                    <td style="padding: 8px;">{run_id}</td>
                </tr>
                <tr>
                    <td style="padding: 8px; background: #f5f5f5;"><b>Time</b></td>
                    <td style="padding: 8px;">{timestamp}</td>
                </tr>
            </table>
            <p>
                <a href="{run_url}" 
                   style="background: #1976d2; color: white; padding: 10px 20px; 
                          text-decoration: none; display: inline-block; margin-top: 10px;">
                    View in Dagster
                </a>
            </p>
        </body>
        </html>
        """
        
        # Envoyer à tous les destinataires
        for email in settings.alert_to_emails:
            send_email_notification(
                smtp_host=settings.smtp_host,
                smtp_port=settings.smtp_port,
                from_email=settings.alert_from_email,
                to_email=email,
                subject=f"❌ ETL Failed: {job_name}",
                body=email_body,
                smtp_user=settings.smtp_user,
                smtp_password=settings.smtp_password,
            )


@success_hook
def alert_on_success(context: HookContext):
    """
    Hook Dagster déclenché sur succès d'un run.
    
    Notification uniquement pour pipeline complet.
    """
    settings = get_settings()
    
    # Notifier uniquement pour job principal
    job_name = context.job_name
    if job_name != "full_etl_pipeline":
        return
    
    run_id = context.run_id
    dagster_url = os.getenv("DAGSTER_URL", "http://172.30.27.14:3000")
    run_url = f"{dagster_url}/runs/{run_id}"
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    title = "✅ ETL Pipeline Succeeded"
    message = f"**Job:** {job_name}\n**Time:** {timestamp}"
    
    facts = {
        "🔧 Job": job_name,
        "🆔 Run ID": run_id[:8] + "...",
        "🕐 Time": timestamp,
        "💻 Server": "S918-ETL-02-FR",
        "✅ Status": "SUCCESS"
    }
    
    # Teams notification
    if settings.teams_webhook_url:
        send_teams_notification(
            webhook_url=settings.teams_webhook_url,
            title=title,
            message=message,
            color="28A745",  # Vert
            facts=facts,
            action_url=run_url,
        )


@success_hook
def metrics_hook(context: HookContext):
    """
    Hook pour collecter métriques de performance.
    
    Logger durée et stats dans PostgreSQL.
    """
    settings = get_settings()
    job_name = context.job_name
    run_id = context.run_id
    
    try:
        # TODO: Récupérer durée depuis context
        # Pour l'instant on log juste le timestamp
        
        conn = psycopg2.connect(settings.postgres_url)
        
        try:
            with conn.cursor() as cur:
                # Créer table metrics si n'existe pas
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS etl_logs.job_metrics (
                        id SERIAL PRIMARY KEY,
                        job_name TEXT NOT NULL,
                        run_id TEXT NOT NULL,
                        duration_seconds INTEGER,
                        success BOOLEAN NOT NULL,
                        timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                # Insérer métrique
                cur.execute("""
                    INSERT INTO etl_logs.job_metrics (
                        job_name, run_id, duration_seconds, success, timestamp
                    )
                    VALUES (%s, %s, %s, true, CURRENT_TIMESTAMP)
                """, (job_name, run_id, None))  # duration à implémenter
                
                conn.commit()
                logger.info(f"[METRICS] Logged success for {job_name}")
        
        finally:
            conn.close()
            
    except Exception as e:
        logger.warning(f"[METRICS] Failed to log: {e}")


# =============================================================================
# Export
# =============================================================================

__all__ = [
    "alert_on_failure",
    "alert_on_success",
    "metrics_hook",
    "send_teams_notification",
    "send_email_notification",
]