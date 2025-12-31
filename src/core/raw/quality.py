"""
============================================================================
RAW Data Quality - Testing et remontée d'erreurs
============================================================================
Système pour identifier les lignes problématiques AVANT le chargement RAW
et générer des rapports de qualité des données
============================================================================
"""

import json
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass, asdict

import pyarrow.parquet as pq
import pandas as pd

from src.utils.logging import get_logger

logger = get_logger(__name__)


@dataclass
class DataQualityIssue:
    """Représente un problème de qualité de données"""
    table_name: str
    row_number: int
    column_name: str
    issue_type: str  # 'special_char', 'encoding', 'length', 'type_mismatch'
    value_sample: str  # Échantillon de la valeur (tronqué)
    severity: str  # 'warning', 'error', 'critical'
    suggestion: str  # Suggestion de correction


@dataclass
class DataQualityReport:
    """Rapport de qualité des données"""
    table_name: str
    file_path: str
    total_rows: int
    total_columns: int
    scan_timestamp: str
    issues: List[DataQualityIssue]
    passed: bool
    
    def to_dict(self):
        return {
            **{k: v for k, v in asdict(self).items() if k != 'issues'},
            'issues': [asdict(i) for i in self.issues],
            'issue_count': len(self.issues),
            'error_count': sum(1 for i in self.issues if i.severity == 'error'),
            'warning_count': sum(1 for i in self.issues if i.severity == 'warning'),
        }


class RawDataQualityChecker:
    """
    Vérifie la qualité des données Parquet AVANT le chargement RAW
    """
    
    def __init__(self, max_issues_per_type: int = 100):
        self.max_issues_per_type = max_issues_per_type
        self.logger = get_logger(__name__)
    
    def check_parquet_file(
        self, 
        parquet_path: Path, 
        table_name: str,
        sample_size: Optional[int] = None
    ) -> DataQualityReport:
        """
        Scanne un fichier Parquet pour détecter les problèmes
        
        Args:
            parquet_path: Chemin du fichier Parquet
            table_name: Nom de la table
            sample_size: Nombre de lignes à scanner (None = tout)
        
        Returns:
            DataQualityReport avec tous les problèmes détectés
        """
        self.logger.info(f"Starting data quality check: {table_name}")
        
        issues = []
        
        # Lire le Parquet
        parquet_file = pq.ParquetFile(parquet_path)
        df = parquet_file.read().to_pandas()
        
        if sample_size and len(df) > sample_size:
            df = df.head(sample_size)
            self.logger.info(f"Sampling {sample_size} rows out of {len(df)}")
        
        total_rows = len(df)
        total_columns = len(df.columns)
        
        # ================================================================
        # VÉRIFICATIONS
        # ================================================================
        
        # 1. Guillemets doubles non échappés
        issues.extend(self._check_unescaped_quotes(df, table_name))
        
        # 2. Caractères de contrôle (tabs, newlines)
        issues.extend(self._check_control_characters(df, table_name))
        
        # 3. Encodage invalide
        issues.extend(self._check_encoding_issues(df, table_name))
        
        # 4. Valeurs trop longues
        issues.extend(self._check_length_issues(df, table_name))
        
        # 5. Délimiteurs dans les données
        issues.extend(self._check_delimiter_issues(df, table_name))
        
        # Déterminer si le fichier passe les tests
        error_count = sum(1 for i in issues if i.severity == 'error')
        passed = error_count == 0
        
        report = DataQualityReport(
            table_name=table_name,
            file_path=str(parquet_path),
            total_rows=total_rows,
            total_columns=total_columns,
            scan_timestamp=datetime.now().isoformat(),
            issues=issues,
            passed=passed
        )
        
        self.logger.info(
            f"Data quality check completed: {table_name}",
            issues=len(issues),
            errors=error_count,
            passed=passed
        )
        
        return report
    
    def _check_unescaped_quotes(
        self, 
        df: pd.DataFrame, 
        table_name: str
    ) -> List[DataQualityIssue]:
        """Détecte les guillemets doubles non échappés"""
        issues = []
        count = 0
        
        for col in df.columns:
            if count >= self.max_issues_per_type:
                break
            
            # Convertir en string
            col_data = df[col].astype(str)
            
            # Trouver les lignes avec guillemets doubles
            mask = col_data.str.contains('"', na=False, regex=False)
            problematic_rows = df[mask].index.tolist()
            
            for row_idx in problematic_rows[:self.max_issues_per_type - count]:
                value = str(df.loc[row_idx, col])
                
                issues.append(DataQualityIssue(
                    table_name=table_name,
                    row_number=row_idx + 1,  # 1-indexed
                    column_name=col,
                    issue_type='unescaped_quotes',
                    value_sample=value[:100] + ('...' if len(value) > 100 else ''),
                    severity='warning',
                    suggestion='Guillemets seront automatiquement échappés lors du chargement'
                ))
                count += 1
        
        return issues
    
    def _check_control_characters(
        self, 
        df: pd.DataFrame, 
        table_name: str
    ) -> List[DataQualityIssue]:
        """Détecte les caractères de contrôle (tabs, newlines)"""
        issues = []
        count = 0
        
        for col in df.columns:
            if count >= self.max_issues_per_type:
                break
            
            col_data = df[col].astype(str)
            
            # Tabs
            mask_tab = col_data.str.contains('\t', na=False, regex=False)
            # Newlines
            mask_nl = col_data.str.contains('\n|\r', na=False, regex=True)
            
            mask = mask_tab | mask_nl
            problematic_rows = df[mask].index.tolist()
            
            for row_idx in problematic_rows[:self.max_issues_per_type - count]:
                value = str(df.loc[row_idx, col])
                
                issues.append(DataQualityIssue(
                    table_name=table_name,
                    row_number=row_idx + 1,
                    column_name=col,
                    issue_type='control_characters',
                    value_sample=repr(value[:100]),  # repr pour voir les \t \n
                    severity='warning',
                    suggestion='Caractères de contrôle seront remplacés par des espaces'
                ))
                count += 1
        
        return issues
    
    def _check_encoding_issues(
        self, 
        df: pd.DataFrame, 
        table_name: str
    ) -> List[DataQualityIssue]:
        """Détecte les problèmes d'encodage"""
        issues = []
        count = 0
        
        for col in df.columns:
            if count >= self.max_issues_per_type:
                break
            
            col_data = df[col].astype(str)
            
            for row_idx, value in enumerate(col_data):
                if count >= self.max_issues_per_type:
                    break
                
                # Essayer d'encoder en UTF-8
                try:
                    value.encode('utf-8')
                except UnicodeEncodeError:
                    issues.append(DataQualityIssue(
                        table_name=table_name,
                        row_number=row_idx + 1,
                        column_name=col,
                        issue_type='encoding_error',
                        value_sample=repr(value[:100]),
                        severity='error',
                        suggestion='Corriger l\'encodage à la source (Progress)'
                    ))
                    count += 1
        
        return issues
    
    def _check_length_issues(
        self, 
        df: pd.DataFrame, 
        table_name: str,
        max_length: int = 100000
    ) -> List[DataQualityIssue]:
        """Détecte les valeurs anormalement longues"""
        issues = []
        count = 0
        
        for col in df.columns:
            if count >= self.max_issues_per_type:
                break
            
            col_data = df[col].astype(str)
            lengths = col_data.str.len()
            
            mask = lengths > max_length
            problematic_rows = df[mask].index.tolist()
            
            for row_idx in problematic_rows[:self.max_issues_per_type - count]:
                value = str(df.loc[row_idx, col])
                length = len(value)
                
                issues.append(DataQualityIssue(
                    table_name=table_name,
                    row_number=row_idx + 1,
                    column_name=col,
                    issue_type='excessive_length',
                    value_sample=f"{value[:100]}... (total: {length} chars)",
                    severity='warning',
                    suggestion=f'Valeur anormalement longue ({length} caractères)'
                ))
                count += 1
        
        return issues
    
    def _check_delimiter_issues(
        self, 
        df: pd.DataFrame, 
        table_name: str
    ) -> List[DataQualityIssue]:
        """Détecte les délimiteurs potentiellement problématiques"""
        issues = []
        
        # Délimiteurs à vérifier
        delimiters = [';', '|', '\t']
        
        for col in df.columns:
            col_data = df[col].astype(str)
            
            for delim in delimiters:
                mask = col_data.str.contains(delim, na=False, regex=False)
                count_delim = mask.sum()
                
                if count_delim > 0:
                    # Juste un warning général, pas ligne par ligne
                    issues.append(DataQualityIssue(
                        table_name=table_name,
                        row_number=0,  # Pas spécifique à une ligne
                        column_name=col,
                        issue_type='delimiter_in_data',
                        value_sample=f"{count_delim} rows contain '{delim}'",
                        severity='info',
                        suggestion=f'Colonne contient le délimiteur "{delim}" (géré automatiquement)'
                    ))
        
        return issues


def generate_quality_report_html(report: DataQualityReport, output_path: Path):
    """Génère un rapport HTML de qualité des données"""
    
    issues_by_type = {}
    for issue in report.issues:
        if issue.issue_type not in issues_by_type:
            issues_by_type[issue.issue_type] = []
        issues_by_type[issue.issue_type].append(issue)
    
    html = f"""
<!DOCTYPE html>
<html>
<head>
    <title>Data Quality Report - {report.table_name}</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; }}
        .header {{ background: #f0f0f0; padding: 20px; border-radius: 5px; }}
        .summary {{ margin: 20px 0; }}
        .passed {{ color: green; font-weight: bold; }}
        .failed {{ color: red; font-weight: bold; }}
        table {{ border-collapse: collapse; width: 100%; margin: 20px 0; }}
        th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
        th {{ background-color: #4CAF50; color: white; }}
        .error {{ background-color: #ffebee; }}
        .warning {{ background-color: #fff3e0; }}
        .info {{ background-color: #e3f2fd; }}
        pre {{ background: #f5f5f5; padding: 10px; overflow-x: auto; }}
    </style>
</head>
<body>
    <div class="header">
        <h1>Data Quality Report</h1>
        <p><strong>Table:</strong> {report.table_name}</p>
        <p><strong>File:</strong> {report.file_path}</p>
        <p><strong>Scan Time:</strong> {report.scan_timestamp}</p>
        <p><strong>Status:</strong> <span class="{'passed' if report.passed else 'failed'}">
            {'✅ PASSED' if report.passed else '❌ FAILED'}
        </span></p>
    </div>
    
    <div class="summary">
        <h2>Summary</h2>
        <ul>
            <li>Total Rows: {report.total_rows:,}</li>
            <li>Total Columns: {report.total_columns}</li>
            <li>Issues Found: {len(report.issues)}</li>
            <li>Errors: {sum(1 for i in report.issues if i.severity == 'error')}</li>
            <li>Warnings: {sum(1 for i in report.issues if i.severity == 'warning')}</li>
        </ul>
    </div>
"""
    
    if report.issues:
        html += "<h2>Issues by Type</h2>"
        
        for issue_type, issues in issues_by_type.items():
            html += f"<h3>{issue_type.replace('_', ' ').title()} ({len(issues)} occurrences)</h3>"
            html += "<table>"
            html += "<tr><th>Row</th><th>Column</th><th>Severity</th><th>Sample Value</th><th>Suggestion</th></tr>"
            
            # Limiter à 50 par type
            for issue in issues[:50]:
                severity_class = issue.severity
                html += f"""
                <tr class="{severity_class}">
                    <td>{issue.row_number if issue.row_number > 0 else 'N/A'}</td>
                    <td>{issue.column_name}</td>
                    <td>{issue.severity.upper()}</td>
                    <td><pre>{issue.value_sample}</pre></td>
                    <td>{issue.suggestion}</td>
                </tr>
                """
            
            if len(issues) > 50:
                html += f"<tr><td colspan='5'><em>... and {len(issues) - 50} more</em></td></tr>"
            
            html += "</table>"
    else:
        html += "<p style='color: green; font-size: 18px;'>✅ No issues found!</p>"
    
    html += """
</body>
</html>
"""
    
    output_path.write_text(html, encoding='utf-8')


def save_quality_report_json(report: DataQualityReport, output_path: Path):
    """Sauvegarde le rapport en JSON"""
    output_path.write_text(
        json.dumps(report.to_dict(), indent=2, ensure_ascii=False),
        encoding='utf-8'
    )