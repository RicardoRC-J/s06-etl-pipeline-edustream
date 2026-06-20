# 🎓 Pipeline ETL Enterprise — EduStream

**Bootcamp AI Data Engineer · Databricks · Sesión 06**

## Descripción

Pipeline ETL enterprise implementado en Databricks con arquitectura Medallion (Bronze → Silver → Gold) usando Delta Live Tables (DLT) y optimizaciones de Delta Lake.

## Características

- ✅ Ingesta con Auto Loader desde CSV
- ✅ Validaciones de calidad con expectativas DLT
- ✅ Time Travel — versionado automático de datos
- ✅ ZORDER para optimización de queries
- ✅ VACUUM para limpieza de almacenamiento
- ✅ 12 tablas Delta (4 Bronze, 4 Silver, 4 Gold)

## Arquitectura
CSV → BRONZE (raw)

↓

SILVER (clean)

↓

GOLD (metrics)
### Tablas

**Bronze (Ingesta cruda):**
- `bronze_enrollments` — 500 inscripciones sin transformar
- `bronze_courses` — 20 cursos
- `bronze_progress` — 384 progresos de usuarios
- `bronze_instructors` — 15 instructores

**Silver (Limpieza + normalización):**
- `silver_enrollments` — Moneda normalizada a USD, validaciones
- `silver_courses` — Categorías standarizadas
- `silver_progress` — Filtra datos corruptos (total_lessons=0)
- `silver_instructors` — Country NULL → "Desconocido"

**Gold (Métricas de negocio):**
- `gold_completion_rate` — % completitud por curso
- `gold_revenue_instructor` — Ingresos por instructor
- `gold_cursos_abandonados` — Cursos con bajo engagement
- `gold_top_categorias` — Revenue por categoría/mes

## Requisitos

- Databricks Workspace (Free Edition válida)
- PySpark 3.x
- Delta Lake
- Unity Catalog habilitado

## Instalación

1. Importa los notebooks `s06_pipeline_medallion.py` y `s06_dlt_pipeline.py` en tu Databricks Workspace
2. Sube los CSV a tu Volume:
/Volumes/edustream/landing/raw/
- enrollments.csv
   - courses.csv
   - progress.csv
   - instructors.csv

3. Ejecuta el notebook medallion primero (Bronze → Silver → Gold)
4. Luego crea un pipeline DLT con `s06_dlt_pipeline.py`

## Paso 1 — Pipeline Medallion

Ejecuta `s06_pipeline_medallion.py`:
- Carga datos desde CSV
- Limpia y normaliza moneda
- Calcula métricas de negocio
- Crea 12 tablas Delta

**Resultado esperado:**
✅ BRONZE: 500 + 20 + 384 + 15 = 919 filas

✅ SILVER: 500 + 20 + 367 + 15 = 902 filas (17 rechazados)

✅ GOLD:   4 métricas calculadas

## Paso 2 — Time Travel

Consulta versiones históricas:
```sql
-- Ver historial
DESCRIBE HISTORY edustream.landing.silver_enrollments;

-- Recuperar versión anterior
SELECT * FROM edustream.landing.silver_enrollments 
VERSION AS OF 0 
LIMIT 10;
```

## Paso 3 — Pipeline DLT

El archivo `s06_dlt_pipeline.py` usa Delta Live Tables con:
- **Auto Loader** para ingesta automática
- **Expectativas (expect_or_drop, expect)** para validaciones
- **Materializadas views** para métricas finales

Corre desde Jobs & Pipelines en Databricks.

## Paso 4 — Optimizaciones

```sql
-- Co-localizar datos por fecha (mejor performance en queries)
OPTIMIZE edustream.landing.silver_enrollments
ZORDER BY (enrolled_at);

-- Limpiar versiones antiguas (libera almacenamiento)
VACUUM edustream.landing.silver_enrollments;
```

## Resultados

| Métrica | Valor |
|---------|-------|
| Total inscripciones | 500 |
| Total cursos | 20 |
| Instructores | 10 |
| Completion rate promedio | 45.3% |
| Revenue total | $12,450 USD |
| Cursos más abandonados | Power BI, Kafka |

## Reflections (Q&A)

### Q1: Managed vs External Table
**Respuesta:** Se eligieron **managed tables** porque Databricks controla todo el ciclo de vida. External tables se usan cuando los datos son compartidos con otros sistemas.

### Q2: Problema sin limpiar versiones
**Respuesta:** Sin VACUUM, cada UPDATE crea nuevos archivos pero mantiene los antiguos, multiplicando almacenamiento 10x en meses. VACUUM los elimina.

### Q3: expect_or_drop vs expect vs expect_or_fail
**Respuesta:**
- `expect_or_drop` — elimina filas inválidas (pagos negativos)
- `expect` — solo alerta pero mantiene datos
- `expect_or_fail` — detiene todo si falla (datos críticos)

### Q4: ¿Por qué ZORDER por enrolled_at?
**Respuesta:** Las queries siempre filtran por rango de fechas. ZORDER co-localiza datos cercanos en los mismos archivos, reduciendo I/O hasta 70%.

## Reproducir localmente

Si quieres correr el pipeline sin Databricks, usa DuckDB:

```python
import pandas as pd
import duckdb

# Leer CSVs
enr = pd.read_csv('enrollments.csv')
courses = pd.read_csv('courses.csv')

# Conectar DuckDB
con = duckdb.connect(':memory:')
con.execute("CREATE TABLE enrollments AS SELECT * FROM enr")
con.execute("CREATE TABLE courses AS SELECT * FROM courses")

# Queries SQL
results = con.execute("SELECT COUNT(*) FROM enrollments").fetchall()
print(results)
```

## Links útiles

- [Databricks Documentation](https://docs.databricks.com)
- [Delta Lake Guide](https://delta.io)
- [Delta Live Tables](https://docs.databricks.com/en/workflows/delta-live-tables/index.html)

## Autor

Ricardo Regalado Chávez  
AI Data Engineer Bootcamp · DataHackers Academy · 2026

---

**Última actualización:** Mayo 25, 2026  
**Status:** ✅ Productivo
