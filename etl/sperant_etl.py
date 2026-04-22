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
        if resp.status_code not in (200, 201, 204):
            log.error("RPC upsert error %s: %s", resp.status_code, resp.text[:500])
            resp.raise_for_status()
        # 204 = No Content (success, function returns void); 200/201 may return JSON
        if resp.status_code != 204 and resp.text:
            result = resp.json()
            processed = result.get("processed", len(batch)) if isinstance(result, dict) else len(batch)
        else:
            processed = len(batch)
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
    Extract lead-level data for a project+month using the COSECHA model.

    Seed universe = first creational touchpoint per cliente_id in this project,
    within the period, across ALL channels (Meta Ads, Manual, Chat, Sala de
    Ventas, Feria). Each lead gets classified with:

      canal_origen       — META_ADS | MANUAL | CHAT | SALA_VENTAS | FERIA | OTRO
      tipo_novedad       — NUEVO | RECAPTURADO
      subclasificacion   — NUEVO | RECAP_MISMO | RECAP_CROSS | RECAP_SILENT

    Historical Meta form-fill (in any project) still populates
    fecha_llegada_meta + utm_* when available — this lets us compute TTL and
    keep Meta attribution even for RECAP_CROSS leads.

    utm_filter (Paraíso sub-campaign) restricts the seed to fblead_ads rows
    matching the filter; other channels are excluded for such projects.

    dni is NULL when it starts with 'auto-' (Sperant placeholder).
    """
    utm_clause = get_utm_clause(utm_filter)

    # For utm-filtered projects (e.g. Paraíso within STRN), the seed must only
    # include Meta rows matching the campaign — non-Meta channels cannot be
    # attributed to the sub-campaign, so we exclude them.
    if utm_filter:
        seed_where = f"""
              i.origen = 'fblead_ads'
          AND i.tipo_interaccion IN ('facebook','creación de cliente')
          {utm_clause.replace('utm_campaign', 'i.utm_campaign')}
        """
    else:
        seed_where = """
              (
                 (i.origen = 'fblead_ads' AND i.tipo_interaccion IN ('facebook','creación de cliente'))
              OR (i.origen = 'manual'     AND i.tipo_interaccion = 'creación de cliente')
              OR (i.origen = 'sperant_chat' AND i.tipo_interaccion = 'creación de cliente')
              OR i.tipo_interaccion IN ('visita al proyecto','visita a feria','visita a oficinas')
              )
        """

    query = f"""
    WITH

    -- 1. Cosecha = first creational touchpoint per cliente_id in THIS project.
    --    Redshift has no DISTINCT ON, use ROW_NUMBER.
    cosecha_ranked AS (
        SELECT
            i.cliente_id,
            i.documento_cliente,
            i.nombres_cliente,
            i.apellidos_cliente,
            i.fecha_creacion,
            i.origen,
            i.tipo_interaccion,
            i.nombres_usuario,
            i.utm_source, i.utm_campaign, i.utm_content, i.utm_medium, i.utm_term,
            ROW_NUMBER() OVER (
                PARTITION BY i.cliente_id
                ORDER BY i.fecha_creacion ASC
            ) AS rn
        FROM tuna.interacciones i
        WHERE i.codigo_proyecto = '{sperant_code}'
          AND ({seed_where})
    ),
    cosecha_periodo AS (
        SELECT *
        FROM cosecha_ranked
        WHERE rn = 1
          AND DATE_PART('year',  fecha_creacion) = {year}
          AND DATE_PART('month', fecha_creacion) = {month}
    ),

    -- 2. First Meta form fill GLOBAL (any project) — gives fecha_llegada_meta
    --    + utm_* even for RECAP_CROSS leads.
    primer_meta_global AS (
        SELECT
            i.cliente_id,
            MIN(i.fecha_creacion)                     AS fecha_llegada_meta,
            MAX(i.utm_source)                         AS utm_source,
            MAX(i.utm_campaign)                       AS utm_campaign,
            MAX(i.utm_content)                        AS utm_content,
            MAX(i.utm_medium)                         AS utm_medium,
            MAX(i.utm_term)                           AS utm_term
        FROM tuna.interacciones i
        INNER JOIN cosecha_periodo cp ON cp.cliente_id = i.cliente_id
        WHERE i.origen = 'fblead_ads'
          AND i.tipo_interaccion = 'facebook'
        GROUP BY i.cliente_id
    ),

    -- 3. First 'creación de cliente' GLOBAL (any project) — anchors tipo_novedad.
    primera_creacion_global AS (
        SELECT
            i.cliente_id,
            MIN(i.fecha_creacion) AS fecha_primera_creacion_global
        FROM tuna.interacciones i
        INNER JOIN cosecha_periodo cp ON cp.cliente_id = i.cliente_id
        WHERE i.tipo_interaccion = 'creación de cliente'
        GROUP BY i.cliente_id
    ),

    -- 4. First 'creación de cliente' in THIS project — anchors subclasificacion.
    primera_creacion_proyecto AS (
        SELECT
            i.cliente_id,
            MIN(i.fecha_creacion) AS fecha_primera_creacion_proyecto
        FROM tuna.interacciones i
        INNER JOIN cosecha_periodo cp ON cp.cliente_id = i.cliente_id
        WHERE i.codigo_proyecto = '{sperant_code}'
          AND i.tipo_interaccion = 'creación de cliente'
        GROUP BY i.cliente_id
    ),

    -- 5. Contact data (phone, email, asesor fallbacks) from tuna.clientes.
    --    For fresh Meta leads the 'creación de cliente' interaction has
    --    nombres_usuario = NULL, so primer_asesor returns NULL.
    --    Fallback chain: ultimo_vendedor → usuario_creador (both from tuna.clientes).
    --    ultimo_vendedor = last asesor to interact; usuario_creador = who registered the lead.
    datos_cliente AS (
        SELECT
            c.id              AS cliente_id,
            COALESCE(NULLIF(TRIM(c.celulares), ''), NULLIF(TRIM(c.telefono), '')) AS celular,
            NULLIF(TRIM(c.email), '')                                              AS email,
            NULLIF(TRIM(c.ultimo_vendedor), '')                                   AS ultimo_vendedor,
            NULLIF(TRIM(c.usuario_creador), '')                                   AS usuario_creador
        FROM tuna.clientes c
        INNER JOIN cosecha_periodo cp ON cp.cliente_id = c.id
    ),

    -- 6. First formal CRM registration in this project (for fecha_creacion_sperant).
    creacion_sperant AS (
        SELECT
            i.cliente_id,
            MIN(i.fecha_creacion) AS fecha_creacion_sperant
        FROM tuna.interacciones i
        INNER JOIN cosecha_periodo cp ON cp.cliente_id = i.cliente_id
        WHERE i.codigo_proyecto  = '{sperant_code}'
          AND i.tipo_interaccion = 'creación de cliente'
        GROUP BY i.cliente_id
    ),

    -- 7. First HUMAN interaction after Meta arrival (for TTL calc).
    primera_humana AS (
        SELECT
            i.cliente_id,
            MIN(i.fecha_creacion) AS fecha_primera_humana
        FROM tuna.interacciones i
        INNER JOIN cosecha_periodo cp ON cp.cliente_id = i.cliente_id
        LEFT JOIN primer_meta_global pmg ON pmg.cliente_id = i.cliente_id
        WHERE i.codigo_proyecto = '{sperant_code}'
          AND pmg.fecha_llegada_meta IS NOT NULL
          AND i.fecha_creacion  > pmg.fecha_llegada_meta
          AND i.tipo_interaccion NOT IN ('facebook', 'creacion de evento', 'api')
        GROUP BY i.cliente_id
    ),

    -- 8. Total interactions per lead in this project.
    total_ints AS (
        SELECT
            i.cliente_id,
            COUNT(*) AS total_interacciones
        FROM tuna.interacciones i
        INNER JOIN cosecha_periodo cp ON cp.cliente_id = i.cliente_id
        WHERE i.codigo_proyecto = '{sperant_code}'
        GROUP BY i.cliente_id
    ),

    -- 9. Latest nivel_interes and razon_desistimiento.
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
        INNER JOIN cosecha_periodo cp ON cp.cliente_id = i.cliente_id
        WHERE i.codigo_proyecto = '{sperant_code}'
    ),
    ultimo_estado_dedup AS (
        SELECT DISTINCT cliente_id, nivel_interes, razon_desistimiento
        FROM ultimo_estado
    ),

    -- 10. Hito flags.
    proformas AS (
        SELECT i.cliente_id, TRUE AS tiene_proforma, MIN(i.fecha_creacion) AS fecha_proforma
        FROM tuna.interacciones i
        INNER JOIN cosecha_periodo cp ON cp.cliente_id = i.cliente_id
        WHERE i.codigo_proyecto = '{sperant_code}' AND i.tipo_interaccion = 'creación de proforma'
        GROUP BY i.cliente_id
    ),
    separaciones AS (
        SELECT i.cliente_id, TRUE AS tiene_separacion, MIN(i.fecha_creacion) AS fecha_separacion
        FROM tuna.interacciones i
        INNER JOIN cosecha_periodo cp ON cp.cliente_id = i.cliente_id
        WHERE i.codigo_proyecto = '{sperant_code}' AND i.nivel_interes = 'separación'
        GROUP BY i.cliente_id
    ),
    ventas AS (
        SELECT i.cliente_id, TRUE AS tiene_venta, MIN(i.fecha_creacion) AS fecha_venta
        FROM tuna.interacciones i
        INNER JOIN cosecha_periodo cp ON cp.cliente_id = i.cliente_id
        WHERE i.codigo_proyecto = '{sperant_code}' AND i.nivel_interes = 'venta'
        GROUP BY i.cliente_id
    ),
    citas_agendadas AS (
        SELECT i.cliente_id, TRUE AS tiene_cita_agendada, MIN(i.fecha_creacion) AS fecha_cita_agendada
        FROM tuna.interacciones i
        INNER JOIN cosecha_periodo cp ON cp.cliente_id = i.cliente_id
        WHERE i.codigo_proyecto = '{sperant_code}' AND LOWER(i.nivel_interes) = 'cita agendada'
        GROUP BY i.cliente_id
    ),
    citas_completadas AS (
        SELECT i.cliente_id, TRUE AS tiene_cita_completada, MIN(i.fecha_creacion) AS fecha_cita_completada
        FROM tuna.interacciones i
        INNER JOIN cosecha_periodo cp ON cp.cliente_id = i.cliente_id
        WHERE i.codigo_proyecto = '{sperant_code}' AND LOWER(i.tipo_interaccion) = 'visita al proyecto'
        GROUP BY i.cliente_id
    ),

    -- 11. First asesor after Meta arrival (falls back to first human in project if no Meta).
    --     For sperant_chat leads, the auto-generated 'creación de cliente' event stores
    --     the agent's system login (e.g. 'eflores') in nombres_usuario instead of their
    --     full display name.  We deprioritise that row so any subsequent human interaction
    --     (call, WhatsApp, visit) — which carries the full name — wins.  If no human
    --     follow-up exists yet the chat-creation row is used as a fallback.
    primer_asesor AS (
        SELECT
            i.cliente_id,
            FIRST_VALUE(i.nombres_usuario)
                OVER (PARTITION BY i.cliente_id
                      ORDER BY
                        CASE WHEN i.origen = 'sperant_chat'
                              AND i.tipo_interaccion = 'creación de cliente'
                             THEN 1 ELSE 0 END ASC,
                        i.fecha_creacion ASC
                      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                ) AS asesor_nombre
        FROM tuna.interacciones i
        INNER JOIN cosecha_periodo cp ON cp.cliente_id = i.cliente_id
        WHERE i.codigo_proyecto = '{sperant_code}'
          AND i.nombres_usuario IS NOT NULL
          AND i.nombres_usuario != ''
          AND i.tipo_interaccion NOT IN ('facebook', 'creacion de evento')
    ),
    primer_asesor_dedup AS (
        SELECT DISTINCT cliente_id, asesor_nombre FROM primer_asesor
    )

    -- Final projection with classifiers.
    SELECT
        cp.cliente_id                                           AS sperant_cliente_id,
        CASE WHEN cp.documento_cliente LIKE 'auto-%' THEN NULL
             ELSE cp.documento_cliente END                      AS dni,
        TRIM(COALESCE(cp.nombres_cliente,'') || ' ' || COALESCE(cp.apellidos_cliente,'')) AS nombre_completo,
        dc.celular,
        dc.email,

        -- Fecha Meta arrival (any project, historical)
        pmg.fecha_llegada_meta,

        -- Fecha cosecha (first touchpoint in THIS project within period)
        cp.fecha_creacion AS fecha_cosecha,

        -- Fecha formal Sperant creation in this project
        cs.fecha_creacion_sperant,

        -- TTL from Meta form → first human, only if Meta exists
        CASE
            WHEN pmg.fecha_llegada_meta IS NOT NULL AND ph.fecha_primera_humana IS NOT NULL
            THEN CAST(DATEDIFF('minute', pmg.fecha_llegada_meta, ph.fecha_primera_humana) AS FLOAT) / 60.0
            ELSE NULL
        END                                                     AS horas_primer_contacto,

        COALESCE(ti.total_interacciones, 0)                     AS total_interacciones,
        ue.nivel_interes,
        ue.razon_desistimiento,
        CASE WHEN ue.nivel_interes = 'desestimado' THEN TRUE ELSE FALSE END AS es_desestimado,
        COALESCE(pf.tiene_proforma,  FALSE)                     AS tiene_proforma,
        COALESCE(sp.tiene_separacion, FALSE)                    AS tiene_separacion,
        COALESCE(vt.tiene_venta,     FALSE)                     AS tiene_venta,
        COALESCE(ca.tiene_cita_agendada, FALSE)                 AS tiene_cita_agendada,
        COALESCE(cc.tiene_cita_completada, FALSE)               AS tiene_cita_completada,

        -- UTM: prefer cosecha row if it's Meta, else fall back to global Meta
        COALESCE(cp.utm_source,   pmg.utm_source)               AS utm_source,
        COALESCE(cp.utm_campaign, pmg.utm_campaign)             AS utm_campaign,
        COALESCE(cp.utm_content,  pmg.utm_content)              AS utm_content,
        COALESCE(cp.utm_medium,   pmg.utm_medium)               AS utm_medium,
        COALESCE(cp.utm_term,     pmg.utm_term)                 AS utm_term,

        -- Asesor: interaction-derived → ultimo_vendedor → usuario_creador
        -- Fresh Meta leads have nombres_usuario=NULL on creación de cliente,
        -- so we fall back to the tuna.clientes fields Sperant uses.
        COALESCE(pa.asesor_nombre, dc.ultimo_vendedor, dc.usuario_creador) AS asesor_nombre,

        pf.fecha_proforma,
        sp.fecha_separacion,
        vt.fecha_venta,
        ca.fecha_cita_agendada,
        cc.fecha_cita_completada,

        -- canal_origen from the cosecha row
        CASE
            WHEN cp.origen = 'fblead_ads'                                       THEN 'META_ADS'
            WHEN cp.origen = 'manual'       AND cp.tipo_interaccion = 'creación de cliente' THEN 'MANUAL'
            WHEN cp.origen = 'sperant_chat' AND cp.tipo_interaccion = 'creación de cliente' THEN 'CHAT'
            WHEN cp.tipo_interaccion IN ('visita al proyecto','visita a oficinas')          THEN 'SALA_VENTAS'
            WHEN cp.tipo_interaccion = 'visita a feria'                          THEN 'FERIA'
            ELSE 'OTRO'
        END                                                                     AS canal_origen,

        -- tipo_novedad: NUEVO only if primera creación global AND en proyecto caen en periodo
        CASE
            WHEN pcg.fecha_primera_creacion_global IS NOT NULL
             AND DATE_PART('year',  pcg.fecha_primera_creacion_global) = {year}
             AND DATE_PART('month', pcg.fecha_primera_creacion_global) = {month}
             AND pcp.fecha_primera_creacion_proyecto IS NOT NULL
             AND DATE_PART('year',  pcp.fecha_primera_creacion_proyecto) = {year}
             AND DATE_PART('month', pcp.fecha_primera_creacion_proyecto) = {month}
            THEN 'NUEVO'
            ELSE 'RECAPTURADO'
        END                                                                     AS tipo_novedad,

        -- subclasificacion: 4-way split
        --   NUEVO        = primera creación global y en proyecto caen en periodo
        --   RECAP_MISMO  = ya era cliente de ESTE proyecto antes del periodo
        --   RECAP_CROSS  = tiene creación de cliente en OTRO proyecto (antes o durante)
        --   RECAP_SILENT = no hay 'creación de cliente' en ningún proyecto
        --                  (cliente_id reusado solo por form-fills crudos)
        CASE
            WHEN pcg.fecha_primera_creacion_global IS NOT NULL
             AND DATE_PART('year',  pcg.fecha_primera_creacion_global) = {year}
             AND DATE_PART('month', pcg.fecha_primera_creacion_global) = {month}
             AND pcp.fecha_primera_creacion_proyecto IS NOT NULL
             AND DATE_PART('year',  pcp.fecha_primera_creacion_proyecto) = {year}
             AND DATE_PART('month', pcp.fecha_primera_creacion_proyecto) = {month}
            THEN 'NUEVO'
            WHEN pcp.fecha_primera_creacion_proyecto IS NOT NULL
             AND pcp.fecha_primera_creacion_proyecto < TO_DATE('{year}-{month:02d}-01','YYYY-MM-DD')
            THEN 'RECAP_MISMO'
            WHEN pcg.fecha_primera_creacion_global IS NULL
            THEN 'RECAP_SILENT'
            ELSE 'RECAP_CROSS'
        END                                                                     AS subclasificacion

    FROM cosecha_periodo cp
    LEFT JOIN datos_cliente              dc  ON dc.cliente_id  = cp.cliente_id
    LEFT JOIN primer_meta_global         pmg ON pmg.cliente_id = cp.cliente_id
    LEFT JOIN primera_creacion_global    pcg ON pcg.cliente_id = cp.cliente_id
    LEFT JOIN primera_creacion_proyecto  pcp ON pcp.cliente_id = cp.cliente_id
    LEFT JOIN creacion_sperant           cs  ON cs.cliente_id  = cp.cliente_id
    LEFT JOIN primera_humana             ph  ON ph.cliente_id  = cp.cliente_id
    LEFT JOIN total_ints                 ti  ON ti.cliente_id  = cp.cliente_id
    LEFT JOIN ultimo_estado_dedup        ue  ON ue.cliente_id  = cp.cliente_id
    LEFT JOIN proformas                  pf  ON pf.cliente_id  = cp.cliente_id
    LEFT JOIN separaciones               sp  ON sp.cliente_id  = cp.cliente_id
    LEFT JOIN ventas                     vt  ON vt.cliente_id  = cp.cliente_id
    LEFT JOIN primer_asesor_dedup        pa  ON pa.cliente_id  = cp.cliente_id
    LEFT JOIN citas_agendadas            ca  ON ca.cliente_id  = cp.cliente_id
    LEFT JOIN citas_completadas          cc  ON cc.cliente_id  = cp.cliente_id
    ORDER BY cp.fecha_creacion
    """

    cur.execute(query)
    rows = cur.fetchall()

    results = []
    # Column order from SELECT (28 cols):
    #  0 sperant_cliente_id  1 dni                 2 nombre_completo
    #  3 celular             4 email
    #  5 fecha_llegada_meta  6 fecha_cosecha       7 fecha_creacion_sperant
    #  8 horas_primer_contacto   9 total_interacciones
    # 10 nivel_interes      11 razon_desistimiento 12 es_desestimado
    # 13 tiene_proforma     14 tiene_separacion    15 tiene_venta
    # 16 tiene_cita_agendada 17 tiene_cita_completada
    # 18 utm_source 19 utm_campaign 20 utm_content 21 utm_medium 22 utm_term
    # 23 asesor_nombre
    # 24 fecha_proforma 25 fecha_separacion 26 fecha_venta
    # 27 fecha_cita_agendada 28 fecha_cita_completada
    # 29 canal_origen 30 tipo_novedad 31 subclasificacion
    for r in rows:
        horas = float(r[8]) if r[8] is not None else None
        if horas is not None and horas < 0:
            horas = None

        results.append({
            "sperant_cliente_id":     int(r[0]) if r[0] else None,
            "dni":                    r[1],
            "nombre_completo":        r[2],
            "celular":                r[3],
            "email":                  r[4],
            "fecha_llegada_meta":     r[5].isoformat() if r[5] else None,
            "fecha_cosecha":          r[6].isoformat() if r[6] else None,
            "fecha_creacion_sperant": r[7].isoformat() if r[7] else None,
            "horas_primer_contacto":  horas,
            "total_interacciones":    int(r[9]) if r[9] else 0,
            "nivel_interes":          r[10],
            "razon_desistimiento":    r[11],
            "es_desestimado":         bool(r[12]),
            "tiene_proforma":         bool(r[13]),
            "tiene_separacion":       bool(r[14]),
            "tiene_venta":            bool(r[15]),
            "tiene_cita_agendada":    bool(r[16]),
            "tiene_cita_completada":  bool(r[17]),
            "utm_source":             r[18],
            "utm_campaign":           r[19],
            "utm_content":            r[20],
            "utm_medium":             r[21],
            "utm_term":               r[22],
            "asesor_nombre":          r[23],
            "fecha_proforma":         r[24].isoformat() if r[24] else None,
            "fecha_separacion":       r[25].isoformat() if r[25] else None,
            "fecha_venta":            r[26].isoformat() if r[26] else None,
            "fecha_cita_agendada":    r[27].isoformat() if r[27] else None,
            "fecha_cita_completada":  r[28].isoformat() if r[28] else None,
            "canal_origen":           r[29],
            "tipo_novedad":           r[30],
            "subclasificacion":       r[31],
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
            "total_nuevos":                0,
            "total_recaptados":            0,
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
            "distribucion_canal_origen":   None,
            "distribucion_subclasificacion": None,
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

    # Nuevos vs recaptados: driven by tipo_novedad classifier (set by Redshift query).
    total_nuevos     = sum(1 for l in leads if l.get("tipo_novedad") == "NUEVO")
    total_recaptados = sum(1 for l in leads if l.get("tipo_novedad") == "RECAPTURADO")
    assert total_nuevos + total_recaptados == n, (
        f"Invariant broken: nuevos={total_nuevos} + recaptados={total_recaptados} != n={n}"
    )

    # Canal origen distribution
    canal_dist: dict[str, int] = {}
    for l in leads:
        c = l.get("canal_origen") or "OTRO"
        canal_dist[c] = canal_dist.get(c, 0) + 1

    # Subclasificacion distribution
    subclas_dist: dict[str, int] = {}
    for l in leads:
        s = l.get("subclasificacion") or "OTRO"
        subclas_dist[s] = subclas_dist.get(s, 0) + 1

    total_proformas    = sum(1 for l in leads if l["tiene_proforma"])
    total_separaciones = sum(1 for l in leads if l["tiene_separacion"])
    total_ventas       = sum(1 for l in leads if l["tiene_venta"])

    return {
        "sperant_codigo":              sperant_code,
        "periodo_anio":                year,
        "periodo_mes":                 month,
        "total_meta_leads":            n,
        "total_creados":               total_creados,
        "total_nuevos":                total_nuevos,
        "total_recaptados":            total_recaptados,
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
        "distribucion_canal_origen":   json.dumps(canal_dist),
        "distribucion_subclasificacion": json.dumps(subclas_dist),
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
