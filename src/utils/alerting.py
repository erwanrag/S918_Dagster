"""Alerting - Notifications Teams & Email"""
import requests
from src.config.settings import get_settings
from src.utils.logging import get_logger

logger = get_logger(__name__)

def send_teams_alert(message: str, level: str = "ERROR"):
    """Envoyer alerte Teams"""
    settings = get_settings()
    webhook = getattr(settings, 'teams_webhook_url', None)
    
    if not webhook:
        return
    
    color = "FF0000" if level == "ERROR" else "FFA500"
    
    payload = {
        "@type": "MessageCard",
        "themeColor": color,
        "title": f"🚨 Dagster {level}",
        "text": message
    }
    
    try:
        requests.post(webhook, json=payload, timeout=5)
        logger.info("Teams alert sent", level=level)
    except Exception as e:
        logger.error("Failed to send Teams alert", error=str(e))
