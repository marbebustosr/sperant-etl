#!/usr/bin/env python3
"""
Sperant CRM → Supabase ETL
===========================
Reads lead interaction data from Sperant's Redshift data warehouse and writes
aggregated KPIs + lead-level details to Supabase for the Sales Compass app.

Runs via GitHub Actions on a daily schedule.

4 indicators extracted per project per month:
  1. Time to Lead (TTL) — hours from Meta form arrival to first human interaction
  2. Total interactions per lead
  3. Nivel de interés (distribution)
  4. Motivos de desistimiento (distribution)

Auth note:
  Uses the Supabase anon key (SUPABASE_KEY env var).
  - sperant_kpis: written directly via REST (anon write RLS policy)
  - sperant_leads: written via RPC upsert_sperant_leads() (SECURITY DEFINER function)

Author: Tuna / Claude (2026-04-08)
"""

import os
import json
import logging
import psycopg2
import requests
from datetime import datetime, timezone
from typing import Optional

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config — read from environment variables (set as GitHub Actions secrets)
# ---------------------------------------------------------------------------
REDSHIFT_HOST     = os.environ.get("REDSHIFT_HOST",     "rssperant-caleb.cmd1cn2chqlh.us-east-1.redshift.amazonaws.com")
REDSHIFT_PORT     = int(os.environ.get("REDSHIFT_PORT", "5439"))
REDSHIFT_DB       = os.environ.get("REDSHIFT_DB",       "q7m2x9htp4wd")
REDSHIFT_USER     = os.environ.get("REDSHIFT_USER",     "xpbc16x4oaq9")
REDSHIFT_PASSWORD = os.environ.get("REDSHIFT_PASSWORD", "")  # REQUIRED — set as secret

SUPABASE_URL      = os.environ.get("SUPABASE_URL",      "https://yvdwgxbkuhifxgwinafs.supabase.co")
SUPABASE_KEY      = os.environ.get("SUPABASE_KEY",      "")  # anon key — set as GitHub secret

# How many months back to process (default: current + 2 months back for safety)
LOOKBACK_MONTHS   = int(os.environ.get("LOOKBACK_MONTHS", "3"))

# ---------------------------------------------------------------------------
# Project mapping: Sperant code → Supabase project_id
# For STRN (Strena), we filter by utm_campaign ILIKE '%paraiso%'
# to isolate the Paraíso campaign (which has its own Supabase project)
# ---------------------------------------------------------------------------
PROJECTS = [
    {
        "sperant_code":  "MELGAR",
        "supabase_id":   "2c7e7418-fa9c-a9b3-b5e1-9777e8f6a7b1",
        "nombre":        "01 Melgar",
        "utm_filter":    None,
    },
    {
        "sperant_code":  "PALACIOS",
        "supabase_id":   "dd68a720-f6c9-b86b-b4b7-c2f14be44567",
        "nombre":        "02 Palacios",
        "utm_filter":    None,
    },
    {
        "sperant_code":  "MA",
        "supabase_id":   "ebb4a7e0-9021-3285-5260-291566f0a11e",
        "nombre":        "03 Monte Alegre",
        "utm_filter":    None,
    },
    {
        "sperant_code":  "STRN",
        "supabase_id":   "77543873-1d68-4eda-bda1-cf0a55883f24",
        "nombre":        "04 Paraiso",
        "utm_filter":    "paraiso",   # utm_campaign ILIKE '%paraiso%'
    },
    {
        "sperant_code":  "GEMMA",
        "supabase_id":   "fbcb0a66-cd16-8bc6-efda-74ba7c6fa704",
        "nombre":        "05 Gemma",
        "utm_filter":    None,
    },
    {
        "sperant_code":  "LF",
        "supabase_id":   "3fbbcbd7-10a3-7d4e-e808-94ad9d50daaf",
        "nombre":        "06 Las Fresas",
        "utm_filter":    None,
    },
    {
        "sperant_code":  "G154",
        "supabase_id":   "6b9a0c80-6617-e2a3-2c2e-1373c734fc47",
        "nombre":        "07 Grid",
        "utm_filter":    None,
    },
    {
        "sperant_code":  "NERVI",
        "supabase_id":   "d3666a4b-1a3f-98fd-cae9-72226c4a2a12",
        "nombre":        "10 Nervi",
        "utm_filter":    None,
    },
    {
        "sperant_code":  "M144",
        "supabase_id":   "5d276280-cf3d-fb0e-f8e8-69ee7f96fb14",
        "nombre":        "11 Morales",
        "utm_filter":    None,
    },
    {
        "sperant_code":  "PREVERDE",
        "supabase_id":   "0bc7bfc5-f2d7-73d3-c1f0-61b74ee73a8b",
        "nombre":        "12 Precursores Verde",
        "utm_filter":    None,
    },
    {
        "sperant_code":  "R125",
        "supabase_id":   "bfd04ac2-9070-37b1-f4b1-5fba42b0b05f",
        "nombre":        "13 Romana",
        "utm_filter":    None,
    },
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def get_lookback_periods(n_months: int) -> list[tuple[int, int]]:
    """Return list of (year, month) tuples for last n_months, most recent first."""
    now = datetime.now(timezone.utc)
    periods = []
    y, m = now.year, now.month
    for _ in range(n_months):
        periods.append((y, m))
        m -= 1
        if m == 0:
            m = 12
            y -= 1
    return periods


def redshift_connect() -> psycopg2.extensions.connection:
    """Open Redshift connection with SSL."""
    if not REDSHIFT_PASSWORD:
        raise ValueError("REDSHIFT_PASSWORD not set")
    return psycopg2.connect(
        host=REDSHIFT_HOST,
        port=REDSHIFT_PORT,
        dbname=REDSHIFT_DB,
        user=REDSHIFT_USER,
        password=REDSHIFT_PASSWORD,
        connect_timeout=30,
        sslmode="require",
    )


def _supabase_headers() -> dict:
    """Standard Supabase REST headers using anon key."""
    if not SUPABASE_KEY:
        raise ValueError("SUPABASE_KEY not set — set it as a GitHub secret")
    return {
        "apikey":        SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type":  "application/json",
    }


def supabase_upsert(table: str, records: list[dict], on_conflict: str) -> None:
    """
    Upsert records into a Supabase table via REST API (anon key).
    Table must have an RLS policy allowing anon writes.
    Used for: sperant_kpis
    """
    if not records:
        return

    url = f"{SUPABASE_URL}/rest/v1/{table}"
    headers = {
        **_supabase_headers(),
        "Prefer": f"resolution=merge-duplicates,on_conflict={on_conflict}",
    }

    batch_size = 500
    total_upserted = 0
    for i in range(0, len(records), batch_size):
        batch = records[i : i + batch_size]
        resp = requests.post(url, headers=headers, data=json.dumps(batch), timeout=60)
        if resp.status_code not in (200, 201):
            log.error("Supabase upsert error %s: %s", resp.status_code, resp.text[:500])
            resp.raise_for_status()
        total_upserted += len(batch)

    log.info("  ✓ Upserted %d rows into %s", total_upserted, table)


def supabase_rpc_upsert_leads(records: list[dict]) -> None:
    """
    Upsert sperant_leads via the upsert_sperant_leads(JSONB) SECURITY DEFINER function.
    This bypasses RLS and runs as postgres — the only way to write to sperant_leads
    without a service_role key (which is held by Lovable, not exposed to the ETL).
    """
    if not records:
        return

    url = f"{SUPABASE_URL}/rest/v1/rpc/upsert_sperant_leads"
    headers = _supabase_headers()

    batch_size = 200  # smaller batches — JSONB RPC is heavier
    total_processed = 0
    for i in range(0, len(records), batch_size):
        batch = records[i : i + batch_size]
        payload = json.dumps({"leads": batch})
        resp = requests.post(url, headers=headers, data=payload, timeout=120)
        if resp.status_code not in (200, 201, 204):  # 204 = void fn success (RETURNS void)
            log.error("RPC upsert error %s: %s", resp.status_code, resp.text[:500])
            resp.raise_for_status()
        if resp.status_code != 204:
            result = resp.json()
            processed = result.get("processed", len(batch)) if isinstance(result, dict) else len(batch)
        else:
            processed = len(batch)  # 204 No Content = void fn returned OK
        total_processed += processed

    log.info("  ✓ Upserted %d lead rows via RPC", total_processed)


# ---------------------------------------------------------------------------
# Core ETL queries
# ---------------------------------------------------------------------------

def get_utm_clause(utm_filter: Optional[str]) -> str:
    """Build optional utm_campaign WHERE clause fragment."""
    if utm_filter:
        return f"AND utm_campaign ILIKE '%{utm_filter}%'"
    return ""


def extract_lead_details(
    cur, sperant_code: str, year: int, month: int, utm_filter: Optional[str]
) -> list[dict]:
    """
    Extract lead-level data for a project+month.
    Returns one dict per unique lead (documento_cliente).

    IMPORTANT: utm_filter is applied ONLY to the initial Meta event (step 1)
    to identify which leads belong to a sub-campaign (e.g. Paraíso within STRN).
    All subsequent interaction queries use the cliente_id list from step 1 —
    they must NOT filter by utm_campaign because human interactions (calls,
    WhatsApp, etc.) never carry utm parameters.

    Columns returned:
      sperant_cliente_id, dni, nombre_completo,
      fecha_llegada_meta, fecha_creacion_sperant,
      horas_primer_contacto, total_interacciones,
      nivel_interes, razon_desistimiento,
      es_desestimado, tiene_proforma, tiene_separacion, tiene_venta
    """
    # utm filter applies ONLY when identifying which leads arrived (step 1)
    utm_clause = get_utm_clause(utm_filter)

    query = f"""
    WITH

    -- 1. Leads that arrived via Facebook Ads in this period
    --    (utm_filter here isolates the sub-campaign, e.g. Paraíso within STRN)
    meta_leads AS (
        SELECT
            cliente_id,
            documento_cliente,
            MAX(nombres_cliente)   AS nombres,
            MAX(apellidos_cliente) AS apellidos,
            MIN(fecha_creacion)    AS fecha_llegada_meta,
            MAX(utm_source)        AS utm_source,
            MAX(utm_campaign)      AS utm_campaign,
            MAX(utm_content)       AS utm_content,
            MAX(utm_medium)        AS utm_medium,
            MAX(utm_term)          AS utm_term
        FROM tuna.interacciones
        WHERE codigo_proyecto = '{sperant_code}'
          AND DATE_PART('year',  fecha_creacion) = {year}
          AND DATE_PART('month', fecha_creacion) = {month}
          AND origen           = 'fblead_ads'
          AND tipo_interaccion = 'facebook'
          {utm_clause}
        GROUP BY cliente_id, documento_cliente
    ),

    -- 2. First formal CRM registration for these leads (no utm filter — registration
    --    events never carry utm_campaign)
    creacion_sperant AS (
        SELECT
            i.cliente_id,
            MIN(i.fecha_creacion) AS fecha_creacion_sperant
        FROM tuna.interacciones i
        INNER JOIN meta_leads ml ON ml.cliente_id = i.cliente_id
        WHERE i.codigo_proyecto    = '{sperant_code}'
          AND i.tipo_interaccion   = 'creaci\u00f3n de cliente'
        GROUP BY i.cliente_id
    ),

    -- 3. First HUMAN interaction after Meta arrival (excludes bot events)
    primera_humana AS (
        SELECT
            i.cliente_id,
            MIN(i.fecha_creacion) AS fecha_primera_humana
        FROM tuna.interacciones i
        INNER JOIN meta_leads ml ON ml.cliente_id = i.cliente_id
        WHERE i.codigo_proyecto = '{sperant_code}'
          AND i.fecha_creacion  > ml.fecha_llegada_meta
          AND i.tipo_interaccion NOT IN ('facebook', 'creacion de evento', 'api')
        GROUP BY i.cliente_id
    ),

    -- 4. Total interactions per lead (all time, for context)
    total_ints AS (
        SELECT
            i.cliente_id,
            COUNT(*) AS total_interacciones
        FROM tuna.interacciones i
        INNER JOIN meta_leads ml ON ml.cliente_id = i.cliente_id
        WHERE i.codigo_proyecto = '{sperant_code}'
        GROUP BY i.cliente_id
    ),

    -- 5. Latest nivel_interes and razon_desistimiento per lead
    ultimo_estado AS (
        SELECT
            i.cliente_id,
            FIRST_VALUE(i.nivel_interes)
                OVER (PARTITION BY i.cliente_id ORDER BY i.fecha_creacion DESC
                      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                ) AS nivel_interes,
            FIRST_VALUE(i.razon_desistimiento)
                OVER (PARTITION BY i.cliente_id ORDER BY i.fecha_creacion DESC
                      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                ) AS razon_desistimiento
        FROM tuna.interacciones i
        INNER JOIN meta_leads ml ON ml.cliente_id = i.cliente_id
        WHERE i.codigo_proyecto = '{sperant_code}'
    ),
    ultimo_estado_dedup AS (
        SELECT DISTINCT cliente_id, nivel_interes, razon_desistimiento
        FROM ultimo_estado
    ),

    -- 6. Proforma flag
    proformas AS (
        SELECT DISTINCT i.cliente_id, TRUE AS tiene_proforma
        FROM tuna.interacciones i
        INNER JOIN meta_leads ml ON ml.cliente_id = i.cliente_id
        WHERE i.codigo_proyecto = '{sperant_code}'
          AND i.tipo_interaccion = 'creaci\u00f3n de proforma'
    ),

    -- 7. Separacion flag (nivel_interes = 'separación')
    separaciones AS (
        SELECT DISTINCT i.cliente_id, TRUE AS tiene_separacion
        FROM tuna.interacciones i
        INNER JOIN meta_leads ml ON ml.cliente_id = i.cliente_id
        WHERE i.codigo_proyecto = '{sperant_code}'
          AND i.nivel_interes = 'separaci\u00f3n'
    ),

    -- 8. Venta flag (nivel_interes = 'venta')
    ventas AS (
        SELECT DISTINCT i.cliente_id, TRUE AS tiene_venta
        FROM tuna.interacciones i
        INNER JOIN meta_leads ml ON ml.cliente_id = i.cliente_id
        WHERE i.codigo_proyecto = '{sperant_code}'
          AND i.nivel_interes = 'venta'
    ),

    -- 9. First asesor (human user) who interacted with each lead
    primer_asesor AS (
        SELECT
            i.cliente_id,
            FIRST_VALUE(i.nombres_usuario)
                OVER (PARTITION BY i.cliente_id ORDER BY i.fecha_creacion ASC
                      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                ) AS asesor_nombre
        FROM tuna.interacciones i
        INNER JOIN meta_leads ml ON ml.cliente_id = i.cliente_id
        WHERE i.codigo_proyecto = '{sperant_code}'
          AND i.fecha_creacion  > ml.fecha_llegada_meta
          AND i.nombres_usuario IS NOT NULL
          AND i.nombres_usuario != ''
          AND i.tipo_interaccion NOT IN ('facebook', 'creacion de evento', 'api')
    ),
    primer_asesor_dedup AS (
        SELECT DISTINCT cliente_id, asesor_nombre FROM primer_asesor
    )

    -- Final join
    SELECT
        ml.cliente_id                                           AS sperant_cliente_id,
        ml.documento_cliente                                    AS dni,
        TRIM(COALESCE(ml.nombres,'') || ' ' || COALESCE(ml.apellidos,'')) AS nombre_completo,
        ml.fecha_llegada_meta,
        cs.fecha_creacion_sperant,
        CASE
            WHEN ph.fecha_primera_humana IS NOT NULL
            THEN CAST(DATEDIFF('minute', ml.fecha_llegada_meta, ph.fecha_primera_humana) AS FLOAT) / 60.0
            ELSE NULL
        END                                                     AS horas_primer_contacto,
        COALESCE(ti.total_interacciones, 0)                    AS total_interacciones,
        ue.nivel_interes,
        ue.razon_desistimiento,
        CASE WHEN ue.nivel_interes = 'desestimado' THEN TRUE ELSE FALSE END AS es_desestimado,
        COALESCE(pf.tiene_proforma,  FALSE)                    AS tiene_proforma,
        COALESCE(sp.tiene_separacion, FALSE)                   AS tiene_separacion,
        COALESCE(vt.tiene_venta,     FALSE)                    AS tiene_venta,
        ml.utm_source,
        ml.utm_campaign,
        ml.utm_content,
        ml.utm_medium,
        ml.utm_term,
        pa.asesor_nombre
    FROM meta_leads ml
    LEFT JOIN creacion_sperant     cs ON cs.cliente_id = ml.cliente_id
    LEFT JOIN primera_humana       ph ON ph.cliente_id = ml.cliente_id
    LEFT JOIN total_ints           ti ON ti.cliente_id = ml.cliente_id
    LEFT JOIN ultimo_estado_dedup  ue ON ue.cliente_id = ml.cliente_id
    LEFT JOIN proformas            pf ON pf.cliente_id = ml.cliente_id
    LEFT JOIN separaciones         sp ON sp.cliente_id = ml.cliente_id
    LEFT JOIN ventas               vt ON vt.cliente_id = ml.cliente_id
    LEFT JOIN primer_asesor_dedup  pa ON pa.cliente_id = ml.cliente_id
    ORDER BY ml.fecha_llegada_meta
    """

    cur.execute(query)
    rows = cur.fetchall()

    results = []
    for r in rows:
        # Filter out negative TTL (leads that had interactions before Meta form)
        horas = float(r[5]) if r[5] is not None else None
        if horas is not None and horas < 0:
            horas = None

        results.append({
            "sperant_cliente_id":     int(r[0]) if r[0] else None,
            "dni":                    r[1],
            "nombre_completo":        r[2],
            "fecha_llegada_meta":     r[3].isoformat() if r[3] else None,
            "fecha_creacion_sperant": r[4].isoformat() if r[4] else None,
            "horas_primer_contacto":  horas,
            "total_interacciones":    int(r[6]) if r[6] else 0,
            "nivel_interes":          r[7],
            "razon_desistimiento":    r[8],
            "es_desestimado":         bool(r[9]),
            "tiene_proforma":         bool(r[10]),
            "tiene_separacion":       bool(r[11]),
            "tiene_venta":            bool(r[12]),
            "utm_source":             r[13],
            "utm_campaign":           r[14],
            "utm_content":            r[15],
            "utm_medium":             r[16],
            "utm_term":               r[17],
            "asesor_nombre":          r[18],
        })

    return results


def compute_kpis(
    leads: list[dict], sperant_code: str, year: int, month: int, cur, utm_filter: Optional[str]
) -> dict:
    """Compute monthly aggregated KPIs from the lead-level data."""
    utm_clause = get_utm_clause(utm_filter)
    n = len(leads)

    if n == 0:
        return {
            "sperant_codigo":              sperant_code,
            "periodo_anio":                year,
            "periodo_mes":                 month,
            "total_meta_leads":            0,
            "total_creados":               0,
            "total_proformas":             0,
            "total_separaciones":          0,
            "total_ventas":                0,
            "ttl_promedio_horas":          None,
            "ttl_pct_menos_1h":            None,
            "ttl_pct_menos_4h":            None,
            "ttl_pct_menos_24h":           None,
            "ttl_pct_sin_respuesta":       None,
            "promedio_interacciones":      None,
            "pct_desestimados":            None,
            "distribucion_nivel_interes":  None,
            "distribucion_desistimiento":  None,
            "updated_at":                  datetime.now(timezone.utc).isoformat(),
        }

    # TTL stats (only leads with valid TTL)
    ttl_values = [l["horas_primer_contacto"] for l in leads if l["horas_primer_contacto"] is not None]
    ttl_respondidos = len(ttl_values)

    ttl_prom   = round(sum(ttl_values) / len(ttl_values), 2) if ttl_values else None
    ttl_1h     = round(sum(1 for t in ttl_values if t <= 1)  / n * 100, 1) if ttl_values else None
    ttl_4h     = round(sum(1 for t in ttl_values if t <= 4)  / n * 100, 1) if ttl_values else None
    ttl_24h    = round(sum(1 for t in ttl_values if t <= 24) / n * 100, 1) if ttl_values else None
    ttl_no_resp = round((n - ttl_respondidos) / n * 100, 1) if n > 0 else None

    # Interaction avg
    total_ints = [l["total_interacciones"] for l in leads]
    int_prom = round(sum(total_ints) / len(total_ints), 1) if total_ints else None

    # Desestimados
    n_desest = sum(1 for l in leads if l["es_desestimado"])
    pct_desest = round(n_desest / n * 100, 1) if n > 0 else None

    # Nivel interés distribution
    nivel_dist: dict[str, int] = {}
    for l in leads:
        ni = l["nivel_interes"] or "sin_dato"
        nivel_dist[ni] = nivel_dist.get(ni, 0) + 1

    # Desistimiento distribution (only desestimados with reason)
    desist_dist: dict[str, int] = {}
    for l in leads:
        rd = l.get("razon_desistimiento")
        if l["es_desestimado"] and rd:
            desist_dist[rd] = desist_dist.get(rd, 0) + 1

    # Count creados (leads that have fecha_creacion_sperant)
    total_creados  = sum(1 for l in leads if l["fecha_creacion_sperant"])
    total_proformas    = sum(1 for l in leads if l["tiene_proforma"])
    total_separaciones = sum(1 for l in leads if l["tiene_separacion"])
    total_ventas       = sum(1 for l in leads if l["tiene_venta"])

    return {
        "sperant_codigo":              sperant_code,
        "periodo_anio":                year,
        "periodo_mes":                 month,
        "total_meta_leads":            n,
        "total_creados":               total_creados,
        "total_proformas":             total_proformas,
        "total_separaciones":          total_separaciones,
        "total_ventas":                total_ventas,
        "ttl_promedio_horas":          ttl_prom,
        "ttl_pct_menos_1h":            ttl_1h,
        "ttl_pct_menos_4h":            ttl_4h,
        "ttl_pct_menos_24h":           ttl_24h,
        "ttl_pct_sin_respuesta":       ttl_no_resp,
        "promedio_interacciones":      int_prom,
        "pct_desestimados":            pct_desest,
        "distribucion_nivel_interes":  json.dumps(nivel_dist),
        "distribucion_desistimiento":  json.dumps(desist_dist) if desist_dist else None,
        "updated_at":                  datetime.now(timezone.utc).isoformat(),
    }




def supabase_rpc_upsert_kpis(records: list[dict]) -> None:
    """
    Upsert sperant_kpis via the upsert_sperant_kpis(JSONB) SECURITY DEFINER function.
    Bypasses RLS — mirrors supabase_rpc_upsert_leads.
    """
    if not records:
        return

    url = f"{SUPABASE_URL}/rest/v1/rpc/upsert_sperant_kpis"
    headers = _supabase_headers()

    batch_size = 200
    total_processed = 0
    for i in range(0, len(records), batch_size):
        batch = records[i : i + batch_size]
        payload = json.dumps({"rows": batch})
        resp = requests.post(url, headers=headers, data=payload, timeout=120)
        if resp.status_code not in (200, 201, 204):  # 204 = void fn success
            log.error("RPC kpis upsert error %s: %s", resp.status_code, resp.text[:500])
            resp.raise_for_status()
        total_processed += len(batch)
        log.info("  ✓ RPC kpis: processed batch %d-%d", i, i + len(batch))

    log.info("  ✓ Upserted %d KPI rows via RPC", total_processed)


# ---------------------------------------------------------------------------
# Main ETL loop
# ---------------------------------------------------------------------------

def run_etl():
    log.info("=== Sperant → Supabase ETL starting ===")
    periods = get_lookback_periods(LOOKBACK_MONTHS)
    log.info("Processing %d periods: %s", len(periods), periods)

    # Connect to Redshift
    log.info("Connecting to Sperant Redshift...")
    conn = redshift_connect()
    cur = conn.cursor()
    log.info("Connected.")

    all_leads_rows  = []
    all_kpis_rows   = []

    for project in PROJECTS:
        code         = project["sperant_code"]
        supabase_id  = project["supabase_id"]
        nombre       = project["nombre"]
        utm_filter   = project["utm_filter"]

        log.info("--- Project: %s (%s) ---", nombre, code)

        for (year, month) in periods:
            period_label = f"{year}-{month:02d}"
            log.info("  Period %s ...", period_label)

            try:
                leads = extract_lead_details(cur, code, year, month, utm_filter)
                log.info("    %d Meta leads found", len(leads))

                if not leads:
                    continue

                # Build lead rows for Supabase
                for lead in leads:
                    row = {
                        "project_id":    supabase_id,
                        "sperant_codigo": code,
                        "periodo_anio":  year,
                        "periodo_mes":   month,
                        "updated_at":    datetime.now(timezone.utc).isoformat(),
                    }
                    row.update(lead)
                    all_leads_rows.append(row)

                # Compute KPIs
                kpi = compute_kpis(leads, code, year, month, cur, utm_filter)
                kpi["project_id"] = supabase_id
                all_kpis_rows.append(kpi)

                log.info(
                    "    TTL avg=%.1fh | creados=%d | proformas=%d | sep=%d | ventas=%d",
                    kpi["ttl_promedio_horas"] or 0,
                    kpi["total_creados"],
                    kpi["total_proformas"],
                    kpi["total_separaciones"],
                    kpi["total_ventas"],
                )

            except Exception as e:
                log.error("    ERROR processing %s %s: %s", code, period_label, e)
                # Reconnect on transaction abort
                try:
                    conn.rollback()
                except Exception:
                    conn.close()
                    conn = redshift_connect()
                    cur = conn.cursor()

    cur.close()
    conn.close()
    log.info("Redshift queries complete. Writing to Supabase...")

    # Upsert leads via SECURITY DEFINER RPC (bypasses RLS without service_role)
    if all_leads_rows:
        log.info("Upserting %d lead rows via RPC...", len(all_leads_rows))
        supabase_rpc_upsert_leads(all_leads_rows)

    # Upsert KPIs via SECURITY DEFINER RPC (bypasses RLS without service_role)
    if all_kpis_rows:
        log.info("Upserting %d KPI rows via RPC...", len(all_kpis_rows))
        supabase_rpc_upsert_kpis(all_kpis_rows)

    log.info("=== ETL complete ===")


if __name__ == "__main__":
    run_etl()
