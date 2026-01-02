"""
Alerting hooks enrichis avec Teams Adaptive Cards + Email HTML
"""
import os  # ✅ Une seule fois

from dagster import (
    RunStatusSensorContext, 
    RunFailureSensorContext,
    DefaultSensorStatus,
    run_status_sensor,
    run_failure_sensor,
    DagsterRunStatus,
    MetadataValue,
    DagsterEventType,  
)
from dagster._core.storage.event_log.base import EventRecordsFilter  
from typing import Dict, Any
import requests
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import smtplib
from datetime import datetime


# ========== RÉCUPÉRATION MÉTRIQUES ==========

def extract_run_metrics(context: RunStatusSensorContext) -> Dict[str, Any]:
    """Extrait TOUTES les métriques depuis les AssetMaterializations du run"""
    
    dagster_url = os.getenv("DAGSTER_URL", "http://localhost:3000")
    
    # ✅ FIX: Utiliser get_event_records au lieu de fetch_materializations
    materializations = []
    try:
        from dagster import DagsterEventType
        from dagster._core.storage.event_log.base import EventRecordsFilter
        
        event_records = context.instance.get_event_records(
            event_records_filter=EventRecordsFilter(
                event_type=DagsterEventType.ASSET_MATERIALIZATION,
                after_cursor=None,
                before_cursor=None,
            ),
            limit=1000
        )
        
        # Filtrer par run_id
        materializations = [
            record for record in event_records
            if record.event_log_entry.run_id == context.dagster_run.run_id
        ]
        
    except Exception as e:
        context.log.warning(f"Could not fetch materializations: {e}")
        materializations = []
    
    # Agrégation
    tables_processed = []
    metrics_by_table = {}
    
    total_rows_loaded = 0
    total_rows_failed = 0
    total_duration = 0.0
    avg_quality = []
    
    for record in materializations:
        try:
            if not hasattr(record, 'event_log_entry'):
                continue
            event = record.event_log_entry.dagster_event
            if not event or not event.asset_materialization:
                continue
                
            mat = event.asset_materialization
            asset_name = mat.asset_key.path[-1]
            metadata = mat.metadata
            
            # Extraction metadata
            rows_loaded = int(metadata.get("rows_loaded", MetadataValue.int(0)).value) if "rows_loaded" in metadata else 0
            rows_failed = int(metadata.get("rows_failed", MetadataValue.int(0)).value) if "rows_failed" in metadata else 0
            duration = float(metadata.get("duration_seconds", MetadataValue.float(0.0)).value) if "duration_seconds" in metadata else 0.0
            quality = metadata.get("quality_score")
            
            tables_processed.append(asset_name)
            metrics_by_table[asset_name] = {
                "rows_loaded": rows_loaded,
                "rows_failed": rows_failed,
                "duration": duration,
                "quality": quality.value if quality else None,
                "status": "✅" if rows_failed == 0 else f"⚠️ ({rows_failed} errors)"
            }
            
            total_rows_loaded += rows_loaded
            total_rows_failed += rows_failed
            total_duration += duration
            if quality:
                avg_quality.append(quality.value)
                
        except Exception as e:
            context.log.warning(f"Error processing materialization: {e}")
            continue

    return {
        "tables_count": len(tables_processed),
        "tables": tables_processed,
        "metrics_by_table": metrics_by_table,
        "total_rows_loaded": total_rows_loaded,
        "total_rows_failed": total_rows_failed,
        "total_duration": total_duration,
        "avg_quality": sum(avg_quality) / len(avg_quality) if avg_quality else None,
        "run_id": context.dagster_run.run_id,
        "run_url": f"{dagster_url}/runs/{context.dagster_run.run_id}"
    }

# ========== TEAMS ADAPTIVE CARD ==========

def build_teams_adaptive_card(context: RunStatusSensorContext, metrics: Dict) -> Dict:
    """Construit une Adaptive Card Teams ultra-riche"""
    
    is_success = context.dagster_run.status == DagsterRunStatus.SUCCESS
    color = "Good" if is_success else "Attention"
    title = "✅ Pipeline Success" if is_success else "❌ Pipeline Failed"
    
    # Construction tableau des tables
    table_rows = [
        {
            "type": "TableRow",
            "cells": [
                {"type": "TableCell", "items": [{"type": "TextBlock", "text": "Table", "weight": "Bolder"}]},
                {"type": "TableCell", "items": [{"type": "TextBlock", "text": "Rows", "weight": "Bolder"}]},
                {"type": "TableCell", "items": [{"type": "TextBlock", "text": "Status", "weight": "Bolder"}]}
            ]
        }
    ]
    
    for table, data in list(metrics["metrics_by_table"].items())[:10]:
        table_rows.append({
            "type": "TableRow",
            "cells": [
                {"type": "TableCell", "items": [{"type": "TextBlock", "text": table}]},
                {"type": "TableCell", "items": [{"type": "TextBlock", "text": f"{data['rows_loaded']:,}"}]},
                {"type": "TableCell", "items": [{"type": "TextBlock", "text": data['status']}]}
            ]
        })
    
    card = {
        "type": "message",
        "attachments": [{
            "contentType": "application/vnd.microsoft.card.adaptive",
            "content": {
                "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                "type": "AdaptiveCard",
                "version": "1.4",
                "body": [
                    {
                        "type": "TextBlock",
                        "text": title,
                        "weight": "Bolder",
                        "size": "Large",
                        "color": color
                    },
                    {
                        "type": "TextBlock",
                        "text": f"Job: {context.dagster_run.job_name}",
                        "wrap": True,
                        "spacing": "None"
                    },
                    {
                        "type": "FactSet",
                        "facts": [
                            {"title": "📊 Tables traitées", "value": str(metrics["tables_count"])},
                            {"title": "✅ Lignes chargées", "value": f"{metrics['total_rows_loaded']:,}"},
                            {"title": "❌ Erreurs", "value": f"{metrics['total_rows_failed']:,}"},
                            {"title": "⏱️ Durée totale", "value": f"{metrics['total_duration']:.1f}s"},
                            {"title": "🎯 Qualité moy.", "value": f"{metrics['avg_quality']:.1%}" if metrics['avg_quality'] else "N/A"},
                            {"title": "🕐 Timestamp", "value": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
                        ],
                        "separator": True
                    },
                    {
                        "type": "TextBlock",
                        "text": "Détails par table:",
                        "weight": "Bolder",
                        "separator": True
                    },
                    {
                        "type": "Table",
                        "columns": [{"width": 2}, {"width": 1}, {"width": 1}],
                        "rows": table_rows
                    }
                ],
                "actions": [
                    {
                        "type": "Action.OpenUrl",
                        "title": "🔗 Voir dans Dagster",
                        "url": metrics["run_url"]
                    }
                ]
            }
        }]
    }
    
    return card


def send_teams_notification(context: RunStatusSensorContext):
    """Envoi notification Teams (Power Automate)"""
    
    webhook_url = os.getenv("TEAMS_WEBHOOK_URL")
    if not webhook_url:
        context.log.warning("TEAMS_WEBHOOK_URL not configured")
        return
    
    metrics = extract_run_metrics(context)
    
    is_success = context.dagster_run.status == DagsterRunStatus.SUCCESS
    title = "✅ Pipeline Success" if is_success else "❌ Pipeline Failed"
    
    # ✅ FORMAT SIMPLE POUR POWER AUTOMATE (pas Adaptive Card)
    simple_message = {
        "title": title,
        "text": f"""
**Job**: {context.dagster_run.job_name}
**Tables**: {metrics['tables_count']}
**Rows**: {metrics['total_rows_loaded']:,}
**Duration**: {metrics['total_duration']:.1f}s
**Status**: {'SUCCESS' if is_success else 'FAILED'}

[View in Dagster]({metrics['run_url']})
        """
    }
    
    try:
        response = requests.post(
            webhook_url,
            json=simple_message,
            headers={"Content-Type": "application/json"},
            timeout=10
        )
        response.raise_for_status()
        context.log.info("Teams notification sent successfully")
    except Exception as e:
        context.log.error(f"Failed to send Teams notification: {e}")


# ========== EMAIL HTML ==========

def build_html_email(context: RunStatusSensorContext, metrics: Dict) -> str:
    """Construit email HTML riche"""
    
    is_success = context.dagster_run.status == DagsterRunStatus.SUCCESS
    header_color = "#28a745" if is_success else "#dc3545"
    title = "✅ Pipeline Success" if is_success else "❌ Pipeline Failed"
    
    # Table HTML
    table_html = ""
    for table, data in metrics["metrics_by_table"].items():
        table_html += f"""
        <tr>
            <td>{table}</td>
            <td>{data['rows_loaded']:,}</td>
            <td>{data['rows_failed']:,}</td>
            <td>{data['duration']:.2f}s</td>
            <td>{data['status']}</td>
        </tr>
        """
    
    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <style>
            body {{ font-family: 'Segoe UI', Arial, sans-serif; margin: 0; padding: 0; background: #f5f5f5; }}
            .container {{ max-width: 800px; margin: 20px auto; background: white; }}
            .header {{ background: {header_color}; color: white; padding: 30px; }}
            .header h1 {{ margin: 0; font-size: 28px; }}
            .metrics {{ display: flex; gap: 20px; padding: 20px; background: #f8f9fa; }}
            .metric-card {{ flex: 1; background: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }}
            .metric-value {{ font-size: 32px; font-weight: bold; color: {header_color}; }}
            .metric-label {{ color: #666; font-size: 14px; margin-top: 5px; }}
            .content {{ padding: 20px; }}
            table {{ width: 100%; border-collapse: collapse; margin-top: 20px; }}
            th {{ background: #007bff; color: white; padding: 12px; text-align: left; }}
            td {{ padding: 12px; border-bottom: 1px solid #ddd; }}
            tr:hover {{ background: #f8f9fa; }}
            .button {{ display: inline-block; background: #007bff; color: white; padding: 12px 24px; 
                      text-decoration: none; border-radius: 5px; margin-top: 20px; }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1>{title}</h1>
                <p style="margin: 10px 0 0 0;">Job: {context.dagster_run.job_name}</p>
            </div>
            
            <div class="metrics">
                <div class="metric-card">
                    <div class="metric-value">{metrics['tables_count']}</div>
                    <div class="metric-label">Tables traitées</div>
                </div>
                <div class="metric-card">
                    <div class="metric-value">{metrics['total_rows_loaded']:,}</div>
                    <div class="metric-label">Lignes chargées</div>
                </div>
                <div class="metric-card">
                    <div class="metric-value">{metrics['total_duration']:.1f}s</div>
                    <div class="metric-label">Durée totale</div>
                </div>
            </div>
            
            <div class="content">
                <h2>Détails par table</h2>
                <table>
                    <thead>
                        <tr>
                            <th>Table</th>
                            <th>Lignes OK</th>
                            <th>Erreurs</th>
                            <th>Durée</th>
                            <th>Status</th>
                        </tr>
                    </thead>
                    <tbody>
                        {table_html}
                    </tbody>
                </table>
                
                <a href="{metrics['run_url']}" class="button">🔗 Voir dans Dagster</a>
            </div>
        </div>
    </body>
    </html>
    """
    
    return html


def send_email_notification(context: RunStatusSensorContext):
    smtp_host = os.getenv("SMTP_HOST")
    smtp_port = int(os.getenv("SMTP_PORT", "25"))
    smtp_user = os.getenv("SMTP_USER")
    smtp_password = os.getenv("SMTP_PASSWORD")
    email_to = os.getenv("ALERT_EMAIL_TO")

    if not all([smtp_host, email_to]):
        context.log.warning("Email config incomplete")
        return

    metrics = extract_run_metrics(context)
    html = build_html_email(context, metrics)

    is_success = context.dagster_run.status == DagsterRunStatus.SUCCESS
    subject = f"[Dagster] {context.dagster_run.job_name} - {'SUCCESS ✅' if is_success else 'FAILED ❌'}"

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = smtp_user or "dagster@cbmcompany.com"
    msg["To"] = email_to
    msg.attach(MIMEText(html, "html"))

    try:
        with smtplib.SMTP(smtp_host, smtp_port, timeout=10) as server:
            # ⚠️ PAS DE TLS SUR PORT 25
            if smtp_user and smtp_password:
                server.login(smtp_user, smtp_password)

            server.send_message(msg)

        context.log.info("Email notification sent successfully")

    except Exception as e:
        context.log.error(f"Failed to send email: {e}")

# ========== SENSORS DAGSTER ==========

# ✅ SANS monitored_jobs = monitore TOUS les jobs
@run_status_sensor(
    run_status=DagsterRunStatus.SUCCESS,
    default_status=DefaultSensorStatus.RUNNING
)
def success_notification_sensor(context: RunStatusSensorContext):
    """Sensor pour runs réussis - monitore TOUS les jobs"""
    send_teams_notification(context)
    send_email_notification(context)


@run_failure_sensor(
    default_status=DefaultSensorStatus.RUNNING
)
def failure_notification_sensor(context: RunFailureSensorContext):
    """Sensor pour runs échoués - monitore TOUS les jobs"""
    send_teams_notification(context)
    send_email_notification(context)