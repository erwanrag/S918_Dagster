"""
============================================================================
Dagster Assets - Services (Devises + Dimension Temporelle)
============================================================================
Réplique les flows Prefect Services :
- load_currency_data_flow() → currency_data_loaded
- build_time_dimension_flow() → time_dimension_built
"""

import sys
sys.path.insert(0, '/data/prefect/projects')

from dagster import (
    asset,
    AssetExecutionContext,
    Output,
    MetadataValue,
)
from typing import Dict, Any
from datetime import datetime, date, timedelta
import calendar
import requests
import psycopg2
from tenacity import retry, stop_after_attempt, wait_exponential

from shared.config import config


# ============================================================================
# CONSTANTS
# ============================================================================

API_CODES_URL = "https://openexchangerates.org/api/currencies.json"
API_RATES_URL = "https://open.er-api.com/v6/latest/EUR"


# ============================================================================
# ASSET 1: Currency Codes (ISO)
# ============================================================================

@asset(
    name="currency_codes_loaded",
    group_name="services",
    description="Charger codes devises ISO 4217",
    compute_kind="api"
)
def currency_codes_loaded(context: AssetExecutionContext) -> Dict[str, Any]:
    """
    Asset: Charger codes devises depuis API et sauvegarder dans PostgreSQL
    
    Équivalent Prefect: fetch_currency_codes() + save_currency_codes()
    
    Returns:
        dict: {
            'codes_loaded': int,
            'source': str
        }
    """
    context.log.info("=" * 70)
    context.log.info("ASSET: Currency Codes")
    context.log.info("=" * 70)
    
    # Fetch depuis API
    try:
        response = requests.get(API_CODES_URL, timeout=30)
        response.raise_for_status()
        codes = response.json()
        context.log.info(f"[API] Fetched {len(codes)} currency codes")
    except Exception as e:
        context.log.error(f"[ERROR] API fetch failed: {e}")
        raise
    
    # Save to PostgreSQL
    conn = psycopg2.connect(config.get_connection_string())
    cur = conn.cursor()
    
    try:
        sql = """
            INSERT INTO reference.currencies (currency_code, currency_name, last_updated)
            VALUES (%s, %s, NOW())
            ON CONFLICT (currency_code) DO UPDATE
            SET currency_name = EXCLUDED.currency_name,
                last_updated = EXCLUDED.last_updated;
        """
        
        for code, name in codes.items():
            cur.execute(sql, (code, name))
        
        conn.commit()
        context.log.info(f"[OK] Saved {len(codes)} currency codes")
        
        yield Output(
            {
                'codes_loaded': len(codes),
                'source': 'openexchangerates.org'
            },
            metadata={
                "num_codes": len(codes),
                "source_api": API_CODES_URL
            }
        )
        
    except Exception as e:
        conn.rollback()
        context.log.error(f"[ERROR] Database save failed: {e}")
        raise
    finally:
        cur.close()
        conn.close()


# ============================================================================
# ASSET 2: Exchange Rates
# ============================================================================

@asset(
    name="exchange_rates_loaded",
    group_name="services",
    description="Charger taux de change quotidiens (base EUR)",
    compute_kind="api",
    deps=["currency_codes_loaded"]
)
def exchange_rates_loaded(context: AssetExecutionContext) -> Dict[str, Any]:
    """
    Asset: Charger taux de change depuis API et sauvegarder
    
    Équivalent Prefect: fetch_exchange_rates() + save_exchange_rates()
    
    Returns:
        dict: {
            'rates_loaded': int,
            'rate_date': str,
            'base_currency': str
        }
    """
    context.log.info("=" * 70)
    context.log.info("ASSET: Exchange Rates")
    context.log.info("=" * 70)
    
    # Fetch depuis API
    try:
        response = requests.get(API_RATES_URL, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        if data.get("result") != "success":
            raise ValueError(f"API Error: {data.get('error-type', 'Unknown')}")
        
        rates = data["rates"]
        rate_date = data.get("time_last_update_utc", datetime.now().strftime("%Y-%m-%d"))
        
        # Parse date si format complexe
        if "," in rate_date:
            from datetime import datetime as dt
            rate_date = dt.strptime(rate_date, "%a, %d %b %Y %H:%M:%S %z").strftime("%Y-%m-%d")
        
        context.log.info(f"[API] Fetched {len(rates)} rates for {rate_date}")
        
    except Exception as e:
        context.log.error(f"[ERROR] API fetch failed: {e}")
        raise
    
    # Save to PostgreSQL
    conn = psycopg2.connect(config.get_connection_string())
    cur = conn.cursor()
    
    try:
        # Récupérer codes valides
        cur.execute("SELECT currency_code FROM reference.currencies")
        valid_codes = {row[0] for row in cur.fetchall()}
        
        # Filtrer rates
        filtered_rates = {k: v for k, v in rates.items() if k in valid_codes and v > 0}
        
        # 1. History table
        sql_hist = """
            INSERT INTO reference.currency_rates (date, currency, rate)
            VALUES (%s, %s, %s)
            ON CONFLICT (date, currency) DO UPDATE SET rate = EXCLUDED.rate;
        """
        for curr, rate in filtered_rates.items():
            cur.execute(sql_hist, (rate_date, curr, rate))
        
        # 2. Snapshot table (today)
        cur.execute("TRUNCATE reference.currency_rates_today;")
        sql_today = "INSERT INTO reference.currency_rates_today (currency, rate, updated_at) VALUES (%s, %s, NOW());"
        for curr, rate in filtered_rates.items():
            cur.execute(sql_today, (curr, rate))
        
        conn.commit()
        context.log.info(f"[OK] Saved {len(filtered_rates)} exchange rates")
        
        yield Output(
            {
                'rates_loaded': len(filtered_rates),
                'rate_date': rate_date,
                'base_currency': 'EUR'
            },
            metadata={
                "num_rates": len(filtered_rates),
                "rate_date": rate_date,
                "base_currency": "EUR",
                "source_api": API_RATES_URL
            }
        )
        
    except Exception as e:
        conn.rollback()
        context.log.error(f"[ERROR] Database save failed: {e}")
        raise
    finally:
        cur.close()
        conn.close()


# ============================================================================
# ASSET 3: Time Dimension
# ============================================================================

def day_suffix(day: int) -> str:
    """Helper: Suffixe jour (st, nd, rd, th)"""
    if 11 <= day <= 13: return "th"
    return {1: "st", 2: "nd", 3: "rd"}.get(day % 10, "th")

def first_of_week(d): return d - timedelta(days=d.weekday())
def last_of_week(d): return first_of_week(d) + timedelta(days=6)
def week_of_month(d): return (d.day - 1) // 7 + 1
def first_of_month(d): return d.replace(day=1)
def last_of_month(d): return (d.replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)
def first_of_quarter(d): return date(d.year, 3 * ((d.month - 1) // 3) + 1, 1)
def last_of_quarter(d): return (first_of_quarter(d) + timedelta(days=92)).replace(day=1) - timedelta(days=1)
def first_of_year(d): return date(d.year, 1, 1)
def last_of_year(d): return date(d.year, 12, 31)


@asset(
    name="time_dimension_built",
    group_name="services",
    description="Construire dimension temporelle 2015-2035",
    compute_kind="postgres"
)
def time_dimension_built(context: AssetExecutionContext) -> Dict[str, Any]:
    """
    Asset: Générer dimension temporelle complète
    
    Équivalent Prefect: generate_time_dimension() dans build_time_dimension_flow()
    
    Période: 2015-01-01 → 2035-12-31 (7,671 jours)
    
    Returns:
        dict: {
            'rows_inserted': int,
            'start_date': str,
            'end_date': str
        }
    """
    context.log.info("=" * 70)
    context.log.info("ASSET: Time Dimension")
    context.log.info("=" * 70)
    
    conn = psycopg2.connect(config.get_connection_string())
    cur = conn.cursor()
    
    try:
        # Truncate table
        context.log.info("[TRUNCATE] Clearing existing data...")
        cur.execute("TRUNCATE reference.time_dimension;")
        conn.commit()
        
        # Définir période
        start = date(2015, 1, 1)
        end = date(2035, 12, 31)
        total_days = (end - start).days + 1
        
        context.log.info(f"[GENERATE] {total_days} days ({start} → {end})")
        
        # SQL Insert
        sql = """
            INSERT INTO reference.time_dimension (
                date_id, day, day_suffix, day_name, day_of_week,
                day_of_week_in_month, day_of_year, is_weekend,
                week, isoweek, first_of_week, last_of_week,
                week_of_month, month, month_name, first_of_month,
                last_of_month, first_of_next_month, last_of_next_month,
                quarter, first_of_quarter, last_of_quarter, year,
                isoyear, first_of_year, last_of_year, is_leap_year,
                has53weeks, has53isoweeks,
                mmyyyy, style101, style103, style112, style120
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
        """
        
        # Générer lignes par batch
        batch = []
        batch_size = 1000
        current = start
        rows_inserted = 0
        
        while current <= end:
            next_m_first = (current.replace(day=28) + timedelta(days=4)).replace(day=1)
            next_m_last = (next_m_first.replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)
            
            row = (
                current, current.day, day_suffix(current.day), current.strftime("%A"), current.weekday() + 1,
                week_of_month(current), current.timetuple().tm_yday, current.weekday() >= 5,
                current.isocalendar()[1], current.isocalendar()[1], first_of_week(current), last_of_week(current),
                week_of_month(current), current.month, current.strftime("%B"), first_of_month(current),
                last_of_month(current), next_m_first, next_m_last,
                (current.month - 1) // 3 + 1, first_of_quarter(current), last_of_quarter(current), current.year,
                current.isocalendar()[0], first_of_year(current), last_of_year(current), calendar.isleap(current.year),
                last_of_year(current).isocalendar()[1] == 53, date(current.year, 12, 28).isocalendar()[1] == 53,
                current.strftime("%m/%Y"), current.strftime("%m/%d/%Y"), current.strftime("%d/%m/%Y"), 
                current.strftime("%Y%m%d"), current.strftime("%Y-%m-%d")
            )
            batch.append(row)
            
            if len(batch) >= batch_size:
                cur.executemany(sql, batch)
                conn.commit()
                rows_inserted += len(batch)
                context.log.info(f"[PROGRESS] {rows_inserted:,} / {total_days:,} rows")
                batch = []
            
            current += timedelta(days=1)
        
        # Insert dernier batch
        if batch:
            cur.executemany(sql, batch)
            conn.commit()
            rows_inserted += len(batch)
        
        context.log.info(f"[OK] Inserted {rows_inserted:,} days")
        
        yield Output(
            {
                'rows_inserted': rows_inserted,
                'start_date': str(start),
                'end_date': str(end)
            },
            metadata={
                "rows_inserted": rows_inserted,
                "start_date": str(start),
                "end_date": str(end),
                "total_days": total_days
            }
        )
        
    except Exception as e:
        conn.rollback()
        context.log.error(f"[ERROR] Time dimension generation failed: {e}")
        raise
    finally:
        cur.close()
        conn.close()