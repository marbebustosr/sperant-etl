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

# TTL on-hours vs off-hours analysis
# Working hours: Tue-Sat 10am-7pm Lima (PET = UTC-5)
# Day of week in Redshift: 0=Sunday, 1=Monday, 2=Tuesday, ..., 6=Saturday
# So working days: 2=Tue, 3=Wed, 4=Thu, 5=Fri, 6=Sat
# Lima time = UTC - 5, so 10am Lima = 15:00 UTC, 7pm Lima = 00:00 UTC next day

query = """
WITH proyectos AS (
  SELECT DISTINCT codigo_proyecto
  FROM tuna.interacciones
  WHERE codigo_proyecto IN ('MELGAR','PALACIOS','MA','STRN','GEMMA','M144')
),
meta_leads AS (
  SELECT
    cliente_id,
    codigo_proyecto,
    MIN(fecha_hora) AS fecha_llegada_meta,
    -- Classify arrival: is it during working hours?
    -- Lima time = UTC-5: 10am-7pm Lima = 15:00-00:00 UTC
    -- Working days: Tue(3)-Sat(7) in DOW where Sun=1,Mon=2,...
    CASE
      WHEN EXTRACT(DOW FROM CONVERT_TIMEZONE('UTC','America/Lima', fecha_hora)) IN (2,3,4,5,6)  -- Tue=2,Wed=3,Thu=4,Fri=5,Sat=6
       AND EXTRACT(HOUR FROM CONVERT_TIMEZONE('UTC','America/Lima', fecha_hora)) >= 10
       AND EXTRACT(HOUR FROM CONVERT_TIMEZONE('UTC','America/Lima', fecha_hora)) < 19
      THEN 'on_hours'
      ELSE 'off_hours'
    END AS horario
  FROM tuna.interacciones
  WHERE tipo_interaccion = 'facebook'
    AND fecha_hora >= DATEADD(month, -3, GETDATE())
  GROUP BY cliente_id, codigo_proyecto
),
primer_contacto AS (
  SELECT
    i.cliente_id,
    i.codigo_proyecto,
    MIN(i.fecha_hora) AS fecha_primer_contacto
  FROM tuna.interacciones i
  WHERE i.tipo_interaccion NOT IN ('facebook','creacion de evento','api')
    AND i.fecha_hora >= DATEADD(month, -3, GETDATE())
  GROUP BY i.cliente_id, i.codigo_proyecto
),
ttl_raw AS (
  SELECT
    m.codigo_proyecto,
    m.horario,
    DATEDIFF(minute, m.fecha_llegada_meta, p.fecha_primer_contacto) / 60.0 AS ttl_horas
  FROM meta_leads m
  LEFT JOIN primer_contacto p
    ON m.cliente_id = p.cliente_id AND m.codigo_proyecto = p.codigo_proyecto
  WHERE p.fecha_primer_contacto IS NULL
     OR p.fecha_primer_contacto >= m.fecha_llegada_meta
)
SELECT
  horario,
  COUNT(*) AS total_leads,
  COUNT(ttl_horas) AS leads_con_contacto,
  ROUND(100.0 * COUNT(ttl_horas) / COUNT(*), 1) AS pct_contactados,
  ROUND(AVG(CASE WHEN ttl_horas >= 0 THEN ttl_horas END), 2) AS ttl_promedio_horas,
  ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY ttl_horas) FILTER (WHERE ttl_horas >= 0), 2) AS ttl_mediana_horas,
  COUNT(CASE WHEN ttl_horas < 1 THEN 1 END) AS menos_1h,
  COUNT(CASE WHEN ttl_horas >= 1 AND ttl_horas < 4 THEN 1 END) AS entre_1_4h,
  COUNT(CASE WHEN ttl_horas >= 4 AND ttl_horas < 24 THEN 1 END) AS entre_4_24h,
  COUNT(CASE WHEN ttl_horas >= 24 THEN 1 END) AS mas_24h,
  COUNT(CASE WHEN ttl_horas IS NULL THEN 1 END) AS sin_contacto_nunca
FROM ttl_raw
GROUP BY horario
ORDER BY horario;
"""

print("=== TTL ON-HOURS vs OFF-HOURS (últimos 3 meses, todos los proyectos TUNA) ===\n")
cur.execute(query)
rows = cur.fetchall()
cols = [d[0] for d in cur.description]
print(f"{'Campo':<30} {'ON_HOURS':>15} {'OFF_HOURS':>15}")
print("-"*62)
data = {r[0]: r for r in rows}
for i, col in enumerate(cols[1:], 1):
    on_val = data.get('on_hours', [None]*15)[i] if 'on_hours' in data else 'N/A'
    off_val = data.get('off_hours', [None]*15)[i] if 'off_hours' in data else 'N/A'
    print(f"{col:<30} {str(on_val):>15} {str(off_val):>15}")

# Per project breakdown
print("\n\n=== TTL PROMEDIO POR PROYECTO (on-hours solamente) ===\n")
query2 = """
WITH meta_leads AS (
  SELECT
    cliente_id,
    codigo_proyecto,
    MIN(fecha_hora) AS fecha_llegada_meta
  FROM tuna.interacciones
  WHERE tipo_interaccion = 'facebook'
    AND fecha_hora >= DATEADD(month, -3, GETDATE())
    AND EXTRACT(DOW FROM CONVERT_TIMEZONE('UTC','America/Lima', fecha_hora)) IN (2,3,4,5,6)
    AND EXTRACT(HOUR FROM CONVERT_TIMEZONE('UTC','America/Lima', fecha_hora)) >= 10
    AND EXTRACT(HOUR FROM CONVERT_TIMEZONE('UTC','America/Lima', fecha_hora)) < 19
    AND codigo_proyecto IN ('MELGAR','PALACIOS','MA','STRN','GEMMA','M144')
  GROUP BY cliente_id, codigo_proyecto
),
primer_contacto AS (
  SELECT
    i.cliente_id, i.codigo_proyecto,
    MIN(i.fecha_hora) AS fecha_primer_contacto
  FROM tuna.interacciones i
  WHERE i.tipo_interaccion NOT IN ('facebook','creacion de evento','api')
    AND i.fecha_hora >= DATEADD(month, -3, GETDATE())
    AND i.codigo_proyecto IN ('MELGAR','PALACIOS','MA','STRN','GEMMA','M144')
  GROUP BY i.cliente_id, i.codigo_proyecto
)
SELECT
  m.codigo_proyecto,
  COUNT(*) AS leads_on_hours,
  ROUND(AVG(CASE WHEN DATEDIFF(minute,m.fecha_llegada_meta,p.fecha_primer_contacto)/60.0 >= 0
                 THEN DATEDIFF(minute,m.fecha_llegada_meta,p.fecha_primer_contacto)/60.0 END), 2) AS ttl_prom_horas,
  ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY DATEDIFF(minute,m.fecha_llegada_meta,p.fecha_primer_contacto)/60.0)
        FILTER (WHERE DATEDIFF(minute,m.fecha_llegada_meta,p.fecha_primer_contacto)/60.0 >= 0), 2) AS ttl_mediana,
  COUNT(CASE WHEN p.fecha_primer_contacto IS NULL THEN 1 END) AS sin_contacto,
  COUNT(CASE WHEN DATEDIFF(minute,m.fecha_llegada_meta,p.fecha_primer_contacto)/60.0 < 1 THEN 1 END) AS menos_1h,
  COUNT(CASE WHEN DATEDIFF(minute,m.fecha_llegada_meta,p.fecha_primer_contacto)/60.0 BETWEEN 1 AND 4 THEN 1 END) AS en_1_4h
FROM meta_leads m
LEFT JOIN primer_contacto p ON m.cliente_id=p.cliente_id AND m.codigo_proyecto=p.codigo_proyecto
GROUP BY m.codigo_proyecto
ORDER BY ttl_prom_horas DESC NULLS FIRST;
"""
cur.execute(query2)
rows2 = cur.fetchall()
cols2 = [d[0] for d in cur.description]
header = "  ".join(f"{c:>18}" for c in cols2)
print(header)
print("-"*len(header))
for row in rows2:
    print("  ".join(f"{str(v):>18}" for v in row))

cur.close()
conn.close()
