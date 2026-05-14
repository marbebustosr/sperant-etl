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
SEP = "=" * 70

# ─────────────────────────────────────────────────────────────
# 1. Distribución de leads por hora del día (Lima) — últimos 3 meses
# ─────────────────────────────────────────────────────────────
q1 = """
SELECT
  DATE_PART('hour', CONVERT_TIMEZONE('UTC','America/Lima', fecha_creacion)) AS hora_lima,
  COUNT(*) AS leads
FROM tuna.interacciones
WHERE codigo_proyecto = 'PALACIOS'
  AND origen = 'fblead_ads'
  AND tipo_interaccion = 'facebook'
  AND fecha_creacion >= DATEADD(month, -3, GETDATE())
GROUP BY 1
ORDER BY 1;
"""
print(f"\n{SEP}")
print("1. LLEGADA DE LEADS POR HORA (Lima) — últimos 3 meses")
print(SEP)
cur.execute(q1)
rows = cur.fetchall()
total = sum(r[1] for r in rows)
working = sum(r[1] for r in rows if 10 <= r[0] < 19)
print(f"{'Hora':>6}  {'Leads':>6}  {'%':>5}  Barra")
print("-" * 45)
for hora, leads in rows:
    bar = "█" * (leads // 5)
    marker = " ← LABORAL" if 10 <= hora < 19 else ""
    print(f"{int(hora):>5}h  {leads:>6}  {100*leads/total:>4.1f}%  {bar}{marker}")
print(f"\nTotal: {total} | En horario laboral: {working} ({100*working/total:.1f}%)")

# ─────────────────────────────────────────────────────────────
# 2. Distribución de nivel de interés actual
# ─────────────────────────────────────────────────────────────
q2 = """
WITH ultima_clasificacion AS (
  SELECT
    i.cliente_id,
    LAST_VALUE(i.nivel_interes) IGNORE NULLS
      OVER (PARTITION BY i.cliente_id ORDER BY i.fecha_creacion
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS nivel
  FROM tuna.interacciones i
  WHERE i.codigo_proyecto = 'PALACIOS'
    AND i.fecha_creacion >= DATEADD(month, -3, GETDATE())
),
dedup AS (
  SELECT DISTINCT cliente_id, nivel FROM ultima_clasificacion
)
SELECT
  COALESCE(nivel, 'sin_clasificar') AS nivel_interes,
  COUNT(*) AS clientes
FROM dedup
GROUP BY 1
ORDER BY clientes DESC;
"""
print(f"\n{SEP}")
print("2. NIVEL DE INTERÉS ACTUAL — PALACIOS (últimos 3 meses)")
print(SEP)
cur.execute(q2)
for row in cur.fetchall():
    print(f"  {row[0]:<30} {row[1]:>5}")

# ─────────────────────────────────────────────────────────────
# 3. Razones de desestimado
# ─────────────────────────────────────────────────────────────
q3 = """
SELECT
  COALESCE(razon_desistimiento, 'sin_razon') AS razon,
  COUNT(DISTINCT cliente_id) AS clientes
FROM tuna.interacciones
WHERE codigo_proyecto = 'PALACIOS'
  AND nivel_interes = 'desestimado'
  AND fecha_creacion >= DATEADD(month, -6, GETDATE())
GROUP BY 1
ORDER BY clientes DESC
LIMIT 15;
"""
print(f"\n{SEP}")
print("3. RAZONES DE DESESTIMADO — PALACIOS (últimos 6 meses)")
print(SEP)
cur.execute(q3)
rows = cur.fetchall()
if rows:
    for row in rows:
        print(f"  {str(row[0]):<45} {row[1]:>5}")
else:
    print("  (sin datos de razón de desestimado)")

# ─────────────────────────────────────────────────────────────
# 4. Tipos de interacción más frecuentes (qué hace el asesor)
# ─────────────────────────────────────────────────────────────
q4 = """
SELECT
  tipo_interaccion,
  COUNT(*) AS cantidad,
  COUNT(DISTINCT cliente_id) AS clientes_distintos
FROM tuna.interacciones
WHERE codigo_proyecto = 'PALACIOS'
  AND fecha_creacion >= DATEADD(month, -3, GETDATE())
  AND tipo_interaccion NOT IN ('facebook','creacion de evento','api')
GROUP BY 1
ORDER BY cantidad DESC
LIMIT 15;
"""
print(f"\n{SEP}")
print("4. TIPOS DE INTERACCIÓN DEL ASESOR — PALACIOS (últimos 3 meses)")
print(SEP)
cur.execute(q4)
print(f"  {'Tipo':<35} {'Total':>7} {'Clientes':>9}")
print("  " + "-"*55)
for row in cur.fetchall():
    print(f"  {str(row[0]):<35} {row[1]:>7} {row[2]:>9}")

# ─────────────────────────────────────────────────────────────
# 5. Velocidad de respuesta por tramo horario de llegada
#    (los 68 leads respondidos en <1h — ¿cuándo llegan?)
# ─────────────────────────────────────────────────────────────
q5 = """
WITH meta_leads AS (
  SELECT
    i.cliente_id,
    MIN(i.fecha_creacion) AS fecha_llegada_meta,
    DATE_PART('hour', CONVERT_TIMEZONE('UTC','America/Lima', MIN(i.fecha_creacion))) AS hora_llegada
  FROM tuna.interacciones i
  WHERE i.origen = 'fblead_ads'
    AND i.tipo_interaccion = 'facebook'
    AND i.codigo_proyecto = 'PALACIOS'
    AND i.fecha_creacion >= DATEADD(month, -3, GETDATE())
    AND DATE_PART('dow',  CONVERT_TIMEZONE('UTC','America/Lima', i.fecha_creacion)) IN (2,3,4,5,6)
    AND DATE_PART('hour', CONVERT_TIMEZONE('UTC','America/Lima', i.fecha_creacion)) >= 10
    AND DATE_PART('hour', CONVERT_TIMEZONE('UTC','America/Lima', i.fecha_creacion)) < 19
  GROUP BY i.cliente_id
),
primer_humano AS (
  SELECT
    i.cliente_id,
    MIN(i.fecha_creacion) AS fecha_primer_contacto
  FROM tuna.interacciones i
  INNER JOIN meta_leads m ON m.cliente_id = i.cliente_id
  WHERE i.tipo_interaccion NOT IN ('facebook','creacion de evento','api')
    AND i.codigo_proyecto = 'PALACIOS'
    AND i.fecha_creacion > m.fecha_llegada_meta
  GROUP BY i.cliente_id
)
SELECT
  m.hora_llegada,
  COUNT(*) AS leads,
  COUNT(CASE WHEN DATEDIFF(minute,m.fecha_llegada_meta,p.fecha_primer_contacto)/60.0 < 1 THEN 1 END) AS menos_1h,
  COUNT(CASE WHEN DATEDIFF(minute,m.fecha_llegada_meta,p.fecha_primer_contacto)/60.0 BETWEEN 1 AND 4 THEN 1 END) AS en_1_4h,
  COUNT(CASE WHEN DATEDIFF(minute,m.fecha_llegada_meta,p.fecha_primer_contacto)/60.0 > 4 THEN 1 END) AS mas_4h,
  COUNT(CASE WHEN p.fecha_primer_contacto IS NULL THEN 1 END) AS sin_contacto,
  ROUND(AVG(CASE WHEN DATEDIFF(minute,m.fecha_llegada_meta,p.fecha_primer_contacto)/60.0 >= 0
            THEN DATEDIFF(minute,m.fecha_llegada_meta,p.fecha_primer_contacto)/60.0 END), 1) AS ttl_prom_h
FROM meta_leads m
LEFT JOIN primer_humano p ON m.cliente_id = p.cliente_id
GROUP BY 1
ORDER BY 1;
"""
print(f"\n{SEP}")
print("5. TTL POR HORA DE LLEGADA (solo on-hours) — PALACIOS")
print(SEP)
cur.execute(q5)
print(f"  {'Hora':>5} {'Leads':>6} {'<1h':>5} {'1-4h':>5} {'>4h':>5} {'Nunca':>6} {'TTL_prom':>9}")
print("  " + "-"*50)
for row in cur.fetchall():
    print(f"  {int(row[0]):>4}h {row[1]:>6} {row[2]:>5} {row[3]:>5} {row[4]:>5} {row[5]:>6} {str(row[6]):>9}h")

# ─────────────────────────────────────────────────────────────
# 6. Asesores activos en PALACIOS (top 5 por interacciones)
# ─────────────────────────────────────────────────────────────
q6 = """
SELECT
  asesor_id,
  asesor_nombre,
  COUNT(DISTINCT cliente_id) AS clientes_atendidos,
  COUNT(*) AS total_interacciones,
  ROUND(AVG(CASE WHEN tipo_interaccion NOT IN ('facebook','creacion de evento','api') THEN 1.0 END * 100), 1) AS pct_interacciones_humanas
FROM tuna.interacciones
WHERE codigo_proyecto = 'PALACIOS'
  AND fecha_creacion >= DATEADD(month, -3, GETDATE())
  AND asesor_id IS NOT NULL
  AND asesor_nombre IS NOT NULL
GROUP BY 1, 2
ORDER BY clientes_atendidos DESC
LIMIT 8;
"""
print(f"\n{SEP}")
print("6. ASESORES EN PALACIOS — últimos 3 meses")
print(SEP)
cur.execute(q6)
rows = cur.fetchall()
if rows:
    cols = [d[0] for d in cur.description]
    print("  " + "  ".join(f"{c:>22}" for c in cols))
    print("  " + "-"*100)
    for row in rows:
        print("  " + "  ".join(f"{str(v):>22}" for v in row))
else:
    print("  (columnas asesor_id/asesor_nombre no disponibles en este schema)")

# ─────────────────────────────────────────────────────────────
# 7. Volumen mensual últimos 6 meses
# ─────────────────────────────────────────────────────────────
q7 = """
SELECT
  DATE_PART('year',  fecha_creacion) AS anio,
  DATE_PART('month', fecha_creacion) AS mes,
  COUNT(DISTINCT cliente_id)         AS leads_unicos
FROM tuna.interacciones
WHERE codigo_proyecto = 'PALACIOS'
  AND origen = 'fblead_ads'
  AND tipo_interaccion = 'facebook'
  AND fecha_creacion >= DATEADD(month, -6, GETDATE())
GROUP BY 1, 2
ORDER BY 1, 2;
"""
print(f"\n{SEP}")
print("7. VOLUMEN MENSUAL — PALACIOS (últimos 6 meses)")
print(SEP)
cur.execute(q7)
for row in cur.fetchall():
    bar = "█" * (row[2] // 10)
    print(f"  {int(row[0])}-{int(row[1]):02d}  {row[2]:>4} leads  {bar}")

cur.close()
conn.close()
print(f"\n{SEP}\nFIN\n{SEP}\n")
