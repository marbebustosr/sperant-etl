-- =============================================================================
-- SPERANT ETL — SUPABASE MIGRATIONS
-- Run this in Supabase Dashboard > SQL Editor BEFORE the first ETL run
-- =============================================================================

-- --------------------------------------------------------
-- 1. ALTER sperant_leads — add all needed columns
--    (table already exists with project_id, we extend it)
-- --------------------------------------------------------

ALTER TABLE sperant_leads
  ADD COLUMN IF NOT EXISTS sperant_cliente_id   INTEGER,
  ADD COLUMN IF NOT EXISTS sperant_codigo        VARCHAR(20),
  ADD COLUMN IF NOT EXISTS periodo_anio          SMALLINT,
  ADD COLUMN IF NOT EXISTS periodo_mes           SMALLINT,
  ADD COLUMN IF NOT EXISTS dni                   VARCHAR(20),
  ADD COLUMN IF NOT EXISTS nombre_completo       VARCHAR(200),
  ADD COLUMN IF NOT EXISTS fecha_llegada_meta    TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS fecha_creacion_sperant TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS horas_primer_contacto  FLOAT,
  ADD COLUMN IF NOT EXISTS total_interacciones    INTEGER DEFAULT 0,
  ADD COLUMN IF NOT EXISTS nivel_interes          VARCHAR(50),
  ADD COLUMN IF NOT EXISTS razon_desistimiento    VARCHAR(200),
  ADD COLUMN IF NOT EXISTS es_desestimado         BOOLEAN DEFAULT FALSE,
  ADD COLUMN IF NOT EXISTS tiene_proforma         BOOLEAN DEFAULT FALSE,
  ADD COLUMN IF NOT EXISTS tiene_separacion       BOOLEAN DEFAULT FALSE,
  ADD COLUMN IF NOT EXISTS tiene_venta            BOOLEAN DEFAULT FALSE,
  ADD COLUMN IF NOT EXISTS created_at             TIMESTAMPTZ DEFAULT NOW(),
  ADD COLUMN IF NOT EXISTS updated_at             TIMESTAMPTZ DEFAULT NOW();

-- Unique constraint: one row per lead per project per month
ALTER TABLE sperant_leads
  DROP CONSTRAINT IF EXISTS sperant_leads_unique_lead;

ALTER TABLE sperant_leads
  ADD CONSTRAINT sperant_leads_unique_lead
  UNIQUE (project_id, periodo_anio, periodo_mes, sperant_cliente_id);

-- --------------------------------------------------------
-- 2. CREATE sperant_kpis — monthly aggregated KPIs per project
-- --------------------------------------------------------

CREATE TABLE IF NOT EXISTS sperant_kpis (
  id                          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  project_id                  UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
  sperant_codigo              VARCHAR(20),
  periodo_anio                SMALLINT NOT NULL,
  periodo_mes                 SMALLINT NOT NULL,

  -- Funnel counts
  total_meta_leads            INTEGER DEFAULT 0,   -- arrived via Facebook Ads
  total_creados               INTEGER DEFAULT 0,   -- registered in CRM
  total_proformas             INTEGER DEFAULT 0,
  total_separaciones          INTEGER DEFAULT 0,
  total_ventas                INTEGER DEFAULT 0,

  -- Time to Lead (TTL)
  ttl_promedio_horas          FLOAT,               -- avg hours to first human contact
  ttl_pct_menos_1h            FLOAT,               -- % responded < 1h
  ttl_pct_menos_4h            FLOAT,               -- % responded < 4h
  ttl_pct_menos_24h           FLOAT,               -- % responded < 24h
  ttl_pct_sin_respuesta       FLOAT,               -- % with no response

  -- Engagement
  promedio_interacciones      FLOAT,               -- avg interactions per lead
  pct_desestimados            FLOAT,               -- % leads marked as desestimado

  -- Distributions (JSON for Lovable charts)
  distribucion_nivel_interes  JSONB,
  -- Example: {"bajo": 45, "no contesta": 30, "intermedio": 15, "alto": 10}

  distribucion_desistimiento  JSONB,
  -- Example: {"No está Interesado": 25, "Fuera de presupuesto": 18, ...}

  -- Metadata
  updated_at                  TIMESTAMPTZ DEFAULT NOW(),

  UNIQUE (project_id, periodo_anio, periodo_mes)
);

-- Enable RLS (same policy as other tables)
ALTER TABLE sperant_kpis ENABLE ROW LEVEL SECURITY;

CREATE POLICY IF NOT EXISTS "Allow read for all" ON sperant_kpis
  FOR SELECT USING (true);

CREATE POLICY IF NOT EXISTS "Allow service role to write" ON sperant_kpis
  FOR ALL USING (auth.role() = 'service_role');

-- Same for sperant_leads
ALTER TABLE sperant_leads ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS "Allow read for all" ON sperant_leads;
CREATE POLICY "Allow read for all" ON sperant_leads
  FOR SELECT USING (true);

DROP POLICY IF EXISTS "Allow service role to write" ON sperant_leads;
CREATE POLICY "Allow service role to write" ON sperant_leads
  FOR ALL USING (auth.role() = 'service_role');

-- --------------------------------------------------------
-- 3. Indexes for query performance in Lovable
-- --------------------------------------------------------

CREATE INDEX IF NOT EXISTS idx_sperant_kpis_project_period
  ON sperant_kpis (project_id, periodo_anio, periodo_mes);

CREATE INDEX IF NOT EXISTS idx_sperant_leads_project_period
  ON sperant_leads (project_id, periodo_anio, periodo_mes);

CREATE INDEX IF NOT EXISTS idx_sperant_leads_nivel_interes
  ON sperant_leads (nivel_interes);

CREATE INDEX IF NOT EXISTS idx_sperant_leads_es_desestimado
  ON sperant_leads (es_desestimado);

-- --------------------------------------------------------
-- Done! Now run the ETL script.
-- --------------------------------------------------------
