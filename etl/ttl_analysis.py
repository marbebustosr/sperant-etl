import os
import psycopg2

conn = psycopg2.connect(
    host=os.environ["REDSHIFT_HOST"],
    port=int(os.environ.get("REDSHIFT_PORT", 5439)),
    dbname=os.environ["REDSHIFT_DB"],
    user=os.environ["REDSHIFT_USER"],
    password=os.environ["REDSHIFT_PASSWORD"],
    connect_timeout=30,
    sslmode="require"
)
cur = conn.cursor()

# ─────────────────────────────────────────────────────────────
# Query 1: TTL on-hours vs off-hours (global, all projects)
# Working hours: Tue-Sat 10am-7pm Lima (UTC-5)
# DOW: 0=Sun 1=Mon 2=Tue 3=Wed 4=Thu 5=Fri 6=Sat
# Tue-Sat = DOW IN (2,3,4,5,6)
# 10am-7pm Lima = hour >= 10 AND hour < 19
# ─────────────────────────────────────────────────────────────
query1 = """
WITH meta_leads AS (
  SELECT
    i.cliente_id,
    i.codigo_proyecto,
    MIN(i.fecha_creacion) AS fecha_llegada_meta,
    CASE
      WHEN DATE_PART('dow', CONVERT_TIMEZONE('UTC','America/Lima', MIN(i.fecha_creacion))) IN (2,3,4,5,6)
       AND DATE_PART('hour', CONVERT_TIMEZONE('UTC','America/Lima', MIN(i.fecha_creacion))) >= 10
       AND DATE_PART('hour', CONVERT_TIMEZONE('UTC','America/Lima', MIN(i.fecha_creacion))) < 19
      THEN 'on_hours'
      ELSE 'off_hours'
    END AS horario
  FROM tuna.interacciones i
  WHERE i.origen = 'fblead_ads'
    AND i.tipo_interaccion = 'facebook'
    AND i.fecha_creacion >= DATEADD(month, -3, GETDATE())
    AND i.codigo_proyecto IN ('MELGAR','PALACIOS','MA','STRN','GEMMA','M144')
  GROUP BY i.cliente_id, i.codigo_proyecto
),
primer_humano AS (
  SELECT
    i.cliente_id,
    i.codigo_proyecto,
    MIN(i.fecha_creacion) AS fecha_primer_contacto
  FROM tuna.interacciones i
  INNER JOIN meta_leads m ON m.cliente_id = i.cliente_id AND m.codigo_proyecto = i.codigo_proyecto
  WHERE i.tipo_interaccion NOT IN ('facebook','creacion de evento','api')
    AND i.fecha_creacion > m.fecha_llegada_meta
  GROUP BY i.cliente_id, i.codigo_proyecto
),
ttl AS (
  SELECT
    m.horario,
    DATEDIFF(minute, m.fecha_llegada_meta, p.fecha_primer_contacto) / 60.0 AS ttl_horas
  FROM meta_leads m
  LEFT JOIN primer_humano p ON m.cliente_id = p.cliente_id AND m.codigo_proyecto = p.codigo_proyecto
)
SELECT
  horario,
  COUNT(*)                                                          AS total_leads,
  COUNT(ttl_horas)                                                  AS leads_con_contacto,
  ROUND(100.0 * COUNT(ttl_horas) / COUNT(*), 1)                    AS pct_contactados,
  ROUND(AVG(CASE WHEN ttl_horas >= 0 THEN ttl_horas END), 2)       AS ttl_promedio_horas,
  ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY
    CASE WHEN ttl_horas >= 0 THEN ttl_horas END), 2)               AS ttl_mediana_horas,
  COUNT(CASE WHEN ttl_horas < 1    THEN 1 END)                     AS menos_1h,
  COUNT(CASE WHEN ttl_horas BETWEEN 1 AND 4   THEN 1 END)          AS entre_1_4h,
  COUNT(CASE WHEN ttl_horas BETWEEN 4 AND 24  THEN 1 END)          AS entre_4_24h,
  COUNT(CASE WHEN ttl_horas > 24   THEN 1 END)                     AS mas_24h,
  COUNT(CASE WHEN ttl_horas IS NULL THEN 1 END)                    AS sin_contacto_nunca
FROM ttl
GROUP BY horario
ORDER BY horario;
"""

print("=" * 70)
print("TTL ON-HOURS vs OFF-HOURS  —  últimos 3 meses, todos los proyectos")
print("Horario laboral: mar-sáb 10am-7pm Lima")
print("=" * 70)
cur.execute(query1)
rows = cur.fetchall()
cols = [d[0] for d in cur.description]
data = {r[0]: r for r in rows}
w = 30
print(f"\n{'':>{w}}  {'ON_HOURS':>12}  {'OFF_HOURS':>12}")
print("-" * (w + 30))
for i, col in enumerate(cols[1:], 1):
    on_val  = data['on_hours'][i]  if 'on_hours'  in data else 'N/A'
    off_val = data['off_hours'][i] if 'off_hours' in data else 'N/A'
    print(f"{col:>{w}}  {str(on_val):>12}  {str(off_val):>12}")

# ─────────────────────────────────────────────────────────────
# Query 2: TTL on-hours by project
# ─────────────────────────────────────────────────────────────
query2 = """
WITH meta_leads AS (
  SELECT
    i.cliente_id,
    i.codigo_proyecto,
    MIN(i.fecha_creacion) AS fecha_llegada_meta
  FROM tuna.interacciones i
  WHERE i.origen = 'fblead_ads'
    AND i.tipo_interaccion = 'facebook'
    AND i.fecha_creacion >= DATEADD(month, -3, GETDATE())
    AND i.codigo_proyecto IN ('MELGAR','PALACIOS','MA','STRN','GEMMA','M144')
    AND DATE_PART('dow',  CONVERT_TIMEZONE('UTC','America/Lima', i.fecha_creacion)) IN (2,3,4,5,6)
    AND DATE_PART('hour', CONVERT_TIMEZONE('UTC','America/Lima', i.fecha_creacion)) >= 10
    AND DATE_PART('hour', CONVERT_TIMEZONE('UTC','America/Lima', i.fecha_creacion)) < 19
  GROUP BY i.cliente_id, i.codigo_proyecto
),
primer_humano AS (
  SELECT
    i.cliente_id,
    i.codigo_proyecto,
    MIN(i.fecha_creacion) AS fecha_primer_contacto
  FROM tuna.interacciones i
  INNER JOIN meta_leads m ON m.cliente_id = i.cliente_id AND m.codigo_proyecto = i.codigo_proyecto
  WHERE i.tipo_interaccion NOT IN ('facebook','creacion de evento','api')
    AND i.fecha_creacion > m.fecha_llegada_meta
  GROUP BY i.cliente_id, i.codigo_proyecto
)
SELECT
  m.codigo_proyecto,
  COUNT(*) AS leads_on_hours,
  COUNT(CASE WHEN p.fecha_primer_contacto IS NULL THEN 1 END) AS sin_contacto,
  ROUND(100.0 * COUNT(CASE WHEN p.fecha_primer_contacto IS NULL THEN 1 END) / COUNT(*), 1) AS pct_sin_contacto,
  ROUND(AVG(CASE WHEN DATEDIFF(minute,m.fecha_llegada_meta,p.fecha_primer_contacto)/60.0 >= 0
                 THEN DATEDIFF(minute,m.fecha_llegada_meta,p.fecha_primer_contacto)/60.0 END), 2) AS ttl_prom_horas,
  ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY
    CASE WHEN DATEDIFF(minute,m.fecha_llegada_meta,p.fecha_primer_contacto)/60.0 >= 0
         THEN DATEDIFF(minute,m.fecha_llegada_meta,p.fecha_primer_contacto)/60.0 END), 2) AS ttl_mediana_horas,
  COUNT(CASE WHEN DATEDIFF(minute,m.fecha_llegada_meta,p.fecha_primer_contacto)/60.0 < 1    THEN 1 END) AS menos_1h,
  COUNT(CASE WHEN DATEDIFF(minute,m.fecha_llegada_meta,p.fecha_primer_contacto)/60.0 BETWEEN 1 AND 4 THEN 1 END) AS en_1_4h,
  COUNT(CASE WHEN DATEDIFF(minute,m.fecha_llegada_meta,p.fecha_primer_contacto)/60.0 > 4   THEN 1 END) AS mas_4h
FROM meta_leads m
LEFT JOIN primer_humano p ON m.cliente_id = p.cliente_id AND m.codigo_proyecto = p.codigo_proyecto
GROUP BY m.codigo_proyecto
ORDER BY ttl_prom_horas DESC NULLS FIRST;
"""

print("\n\n" + "=" * 70)
print("TTL POR PROYECTO  —  solo leads que llegaron EN HORARIO LABORAL")
print("=" * 70 + "\n")
cur.execute(query2)
rows2 = cur.fetchall()
cols2 = [d[0] for d in cur.description]
print("  ".join(f"{c:>16}" for c in cols2))
print("-" * (18 * len(cols2)))
for row in rows2:
    print("  ".join(f"{str(v):>16}" for v in row))

# ─────────────────────────────────────────────────────────────
# Query 3: Distribution of arrival by day of week (Lima time)
# ─────────────────────────────────────────────────────────────
query3 = """
SELECT
  DATE_PART('dow', CONVERT_TIMEZONE('UTC','America/Lima', fecha_creacion)) AS dow,
  CASE DATE_PART('dow', CONVERT_TIMEZONE('UTC','America/Lima', fecha_creacion))
    WHEN 0 THEN 'Dom'
    WHEN 1 THEN 'Lun'
    WHEN 2 THEN 'Mar'
    WHEN 3 THEN 'Mie'
    WHEN 4 THEN 'Jue'
    WHEN 5 THEN 'Vie'
    WHEN 6 THEN 'Sab'
  END AS dia,
  CASE
    WHEN DATE_PART('dow', CONVERT_TIMEZONE('UTC','America/Lima', fecha_creacion)) IN (2,3,4,5,6)
     AND DATE_PART('hour', CONVERT_TIMEZONE('UTC','America/Lima', fecha_creacion)) BETWEEN 10 AND 18
    THEN 'laboral' ELSE 'fuera' END AS tipo,
  COUNT(*) AS leads
FROM tuna.interacciones
WHERE origen = 'fblead_ads'
  AND tipo_interaccion = 'facebook'
  AND fecha_creacion >= DATEADD(month, -3, GETDATE())
  AND codigo_proyecto IN ('MELGAR','PALACIOS','MA','STRN','GEMMA','M144')
GROUP BY 1, 2, 3
ORDER BY 1;
"""

print("\n\n" + "=" * 70)
print("DISTRIBUCIÓN DE LLEGADA DE LEADS POR DÍA (últimos 3 meses)")
print("=" * 70 + "\n")
cur.execute(query3)
rows3 = cur.fetchall()
print(f"{'dow':>5}  {'dia':>5}  {'tipo':>8}  {'leads':>8}")
print("-" * 35)
for row in rows3:
    print(f"{str(row[0]):>5}  {str(row[1]):>5}  {str(row[2]):>8}  {str(row[3]):>8}")

cur.close()
conn.close()
print("\n=== FIN ===")
