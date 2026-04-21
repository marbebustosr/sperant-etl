import os
import psycopg2

conn = psycopg2.connect(
    host=os.environ["REDSHIFT_HOST"],
    port=os.environ["REDSHIFT_PORT"],
    dbname=os.environ["REDSHIFT_DB"],
    user=os.environ["REDSHIFT_USER"],
    password=os.environ["REDSHIFT_PASSWORD"],
)
cur = conn.cursor()

queries = {
    "Q1 - tipo_interaccion distribution MELGAR ultimos 6m": """
        SELECT LOWER(tipo_interaccion) AS tipo, COUNT(*) AS n
        FROM tuna.interacciones
        WHERE codigo_proyecto = 'MELGAR'
          AND fecha_creacion >= '2025-10-01'
        GROUP BY 1 ORDER BY 2 DESC
    """,
    "Q2 - nivel_interes distribution MELGAR ultimos 6m": """
        SELECT LOWER(nivel_interes) AS nivel, COUNT(*) AS n
        FROM tuna.interacciones
        WHERE codigo_proyecto = 'MELGAR'
          AND fecha_creacion >= '2025-10-01'
        GROUP BY 1 ORDER BY 2 DESC
    """,
    "Q3 - matches sospechosos cita/venta/separacion MELGAR ultimos 6m": """
        SELECT LOWER(tipo_interaccion) AS tipo,
               LOWER(nivel_interes) AS nivel,
               COUNT(*) AS n
        FROM tuna.interacciones
        WHERE codigo_proyecto = 'MELGAR'
          AND fecha_creacion >= '2025-10-01'
          AND (LOWER(tipo_interaccion) LIKE '%cita%'
               OR LOWER(tipo_interaccion) LIKE '%visita%'
               OR LOWER(tipo_interaccion) LIKE '%reuni%'
               OR LOWER(nivel_interes) LIKE '%separ%'
               OR LOWER(nivel_interes) LIKE '%vent%'
               OR LOWER(nivel_interes) LIKE '%reserv%'
               OR LOWER(nivel_interes) LIKE '%cierre%')
        GROUP BY 1, 2 ORDER BY 3 DESC
    """,
    "Q4 - todos los codigo_proyecto distintos (para confirmar MELGAR)": """
        SELECT codigo_proyecto, COUNT(*) AS n
        FROM tuna.interacciones
        WHERE fecha_creacion >= '2025-10-01'
        GROUP BY 1 ORDER BY 2 DESC
    """,
}

for label, sql in queries.items():
    print(f"\n{'='*80}\n{label}\n{'='*80}")
    cur.execute(sql)
    for row in cur.fetchall():
        print(row)

cur.close()
conn.close()
