# Sperant → Supabase ETL

Pipeline que extrae datos de leads de Sperant CRM (Redshift) y los carga en Supabase para visualización en Sales Compass.

## ¿Qué extrae?

Por cada proyecto × mes, calcula **4 indicadores clave**:

| Indicador | Tabla Supabase | Descripción |
|---|---|---|
| **Time to Lead (TTL)** | `sperant_kpis` | Horas desde que llega el form de Meta hasta la primera interacción humana |
| **Interacciones por lead** | `sperant_kpis` | Promedio de interacciones registradas en Sperant por lead |
| **Nivel de interés** | `sperant_kpis` | Distribución: bajo / intermedio / alto / desestimado / etc. |
| **Motivos de desistimiento** | `sperant_kpis` | Ranking de razones por las que los leads son descartados |

También carga el detalle de cada lead en `sperant_leads` para análisis one-to-one.

## Proyectos configurados

| Sperant Code | Supabase Project | Filtro |
|---|---|---|
| MELGAR | 01 Melgar | — |
| PALACIOS | 02 Palacios | — |
| MA | 03 Monte Alegre | — |
| STRN | 04 Paraiso | `utm_campaign ILIKE '%paraiso%'` |
| GEMMA | 05 Gemma | — |
| M144 | 11 Morales | — |

---

## Setup inicial (una sola vez)

### 1. Correr la migración SQL en Supabase

Abre **Supabase Dashboard → SQL Editor** y ejecuta el contenido de [`etl/migrations.sql`](etl/migrations.sql).

Esto:
- Extiende la tabla `sperant_leads` con las columnas del ETL
- Crea la tabla `sperant_kpis` con los KPIs mensuales agregados
- Configura RLS (solo service_role puede escribir, todos pueden leer)

### 2. Crear el repositorio en GitHub

```bash
# Clona este folder en un repo privado de GitHub
git init
git add .
git commit -m "init: Sperant ETL"
git remote add origin https://github.com/TU_ORG/sperant-etl.git
git push -u origin main
```

### 3. Agregar los Secrets en GitHub

Ve a **GitHub → Settings → Secrets and variables → Actions → New repository secret**:

| Secret | Valor | Dónde conseguirlo |
|---|---|---|
| `REDSHIFT_HOST` | `rssperant-caleb.cmd1cn2chqlh.us-east-1.redshift.amazonaws.com` | Ya conocido |
| `REDSHIFT_PORT` | `5439` | Ya conocido |
| `REDSHIFT_DB` | `q7m2x9htp4wd` | Ya conocido |
| `REDSHIFT_USER` | `xpbc16x4oaq9` | Ya conocido |
| `REDSHIFT_PASSWORD` | `wEfiu0OhSA98==` | Ya conocido |
| `SUPABASE_URL` | `https://yvdwgxbkuhifxgwinafs.supabase.co` | Ya conocido |
| `SUPABASE_SERVICE_ROLE_KEY` | `eyJ...` | **Ver abajo** ↓ |

**Cómo obtener el `SUPABASE_SERVICE_ROLE_KEY`:**
1. Abre [Supabase Dashboard](https://supabase.com/dashboard)
2. Selecciona el proyecto `yvdwgxbkuhifxgwinafs`
3. Ve a **Settings → API**
4. Copia la clave `service_role` (es distinta a la `anon` key — tiene acceso total)

> ⚠️ La `service_role` key bypassea Row Level Security. Nunca la expongas en el frontend.

### 4. Primer run manual

Una vez configurados los secrets:
1. Ve a **GitHub → Actions → Sperant → Supabase ETL**
2. Clic en **"Run workflow"**
3. Deja `lookback_months = 3` para cargar ene-feb-mar-abr 2026
4. Verifica en Supabase que las tablas tienen datos

---

## Schedule

El ETL corre automáticamente **todos los días a las 6:00 AM Lima** (11:00 UTC).

Para cambiar la frecuencia, edita el `cron` en `.github/workflows/sperant_etl.yml`.

## Re-procesar un período específico

Desde GitHub Actions → **Run workflow** → cambia `lookback_months` al número de meses que quieras reprocesar.

---

## Tablas Supabase

### `sperant_kpis` — KPIs mensuales agregados

```sql
project_id              UUID       FK → projects.id
sperant_codigo          VARCHAR    'MELGAR', 'PALACIOS', etc.
periodo_anio            INT        2026
periodo_mes             INT        4
total_meta_leads        INT        leads llegados de Facebook Ads
total_creados           INT        leads registrados en CRM
total_proformas         INT        
total_separaciones      INT
total_ventas            INT
ttl_promedio_horas      FLOAT      avg horas de respuesta
ttl_pct_menos_1h        FLOAT      % respondidos en <1h
ttl_pct_menos_4h        FLOAT      % respondidos en <4h
ttl_pct_menos_24h       FLOAT      % respondidos en <24h
ttl_pct_sin_respuesta   FLOAT      % sin ninguna respuesta
promedio_interacciones  FLOAT      avg interacciones por lead
pct_desestimados        FLOAT      % leads marcados desestimado
distribucion_nivel_interes  JSONB  {"bajo": 45, "alto": 10, ...}
distribucion_desistimiento  JSONB  {"No está Interesado": 25, ...}
```

### `sperant_leads` — detalle por lead

```sql
project_id              UUID
sperant_cliente_id      INT        ID único en Sperant
sperant_codigo          VARCHAR
periodo_anio, mes       INT
dni                     VARCHAR
nombre_completo         VARCHAR
fecha_llegada_meta      TIMESTAMPTZ
fecha_creacion_sperant  TIMESTAMPTZ
horas_primer_contacto   FLOAT      NULL si nunca fue contactado
total_interacciones     INT
nivel_interes           VARCHAR    último estado
razon_desistimiento     VARCHAR    último motivo si fue desestimado
es_desestimado          BOOLEAN
tiene_proforma          BOOLEAN
tiene_separacion        BOOLEAN
tiene_venta             BOOLEAN
```

---

## Lógica de TTL

El **Time to Lead** mide desde que llega el evento `facebook` (origen=`fblead_ads`) hasta la **primera interacción humana real** (llamada, WhatsApp, email, visita — excluyendo `creacion de evento` y `api`).

- TTL negativo → el lead ya existía en CRM antes de llenar el form → se omite (no aplica)
- TTL `NULL` → el lead llegó pero nunca fue contactado → cuenta en `ttl_pct_sin_respuesta`

---

## Agregar un nuevo proyecto

Edita el array `PROJECTS` en `etl/sperant_etl.py`:

```python
{
    "sperant_code":  "NUEVO_CODIGO",    # código en Sperant
    "supabase_id":   "uuid-del-proyecto-en-supabase",
    "nombre":        "XX Nombre",
    "utm_filter":    None,              # o "paraiso" si es campaña filtrada
},
```
