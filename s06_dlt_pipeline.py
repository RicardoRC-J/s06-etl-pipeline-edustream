"""
Delta Live Tables Pipeline — EduStream
Sesión 06 · Bootcamp AI Data Engineer
"""

import dlt
from pyspark.sql.functions import col, round as spark_round, avg, sum as spark_sum, count

# ════════════════════════════════════════════════════════════════════════════════
# 🥉 BRONZE — Ingesta con Auto Loader
# ════════════════════════════════════════════════════════════════════════════════

@dlt.table(
    name="bronze_enrollments_dlt",
    comment="Ingesta cruda de enrollments desde CSV con Auto Loader"
)
def bronze_enrollments_dlt():
    """Lee CSVs automáticamente desde el Volume"""
    return (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation",
                "/Volumes/edustream/landing/raw/_schema")
        .option("header", "true")
        .load("/Volumes/edustream/landing/raw/enrollments.csv"))

@dlt.table(
    name="bronze_courses_dlt",
    comment="Catálogo de cursos sin transformar"
)
def bronze_courses_dlt():
    return (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation",
                "/Volumes/edustream/landing/raw/_schema")
        .option("header", "true")
        .load("/Volumes/edustream/landing/raw/courses.csv"))

@dlt.table(
    name="bronze_progress_dlt",
    comment="Progreso de usuarios sin transformar"
)
def bronze_progress_dlt():
    return (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation",
                "/Volumes/edustream/landing/raw/_schema")
        .option("header", "true")
        .load("/Volumes/edustream/landing/raw/progress.csv"))

@dlt.table(
    name="bronze_instructors_dlt",
    comment="Datos de instructores sin transformar"
)
def bronze_instructors_dlt():
    return (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation",
                "/Volumes/edustream/landing/raw/_schema")
        .option("header", "true")
        .load("/Volumes/edustream/landing/raw/instructors.csv"))

# ════════════════════════════════════════════════════════════════════════════════
# 🥈 SILVER — Limpieza con expectativas de calidad
# ════════════════════════════════════════════════════════════════════════════════

@dlt.table(
    name="silver_enrollments_dlt",
    comment="Enrollments limpios con validaciones DLT"
)
@dlt.expect_or_drop("pago_valido", "CAST(payment_amount AS DOUBLE) >= 0")
@dlt.expect("course_id_no_nulo", "course_id IS NOT NULL")
def silver_enrollments_dlt():
    """Limpia pagos negativos, normaliza moneda"""
    return (dlt.read_stream("bronze_enrollments_dlt")
        .filter(col("payment_amount").isNotNull()))

@dlt.table(
    name="silver_courses_dlt",
    comment="Cursos con categorías estandarizadas"
)
@dlt.expect("course_id_existe", "course_id IS NOT NULL")
def silver_courses_dlt():
    """Estandariza categorías vacías"""
    return (dlt.read_stream("bronze_courses_dlt")
        .withColumn("category",
            when((col("category") == "") | col("category").isNull(), "Sin categoría")
            .otherwise(col("category"))))

@dlt.table(
    name="silver_progress_dlt",
    comment="Progreso sin datos corruptos"
)
@dlt.expect_or_drop("total_lessons_valido", "CAST(total_lessons AS INT) > 0")
def silver_progress_dlt():
    """Filtra datos corruptos donde total_lessons = 0"""
    return (dlt.read_stream("bronze_progress_dlt")
        .filter(col("total_lessons") > 0))

@dlt.table(
    name="silver_instructors_dlt",
    comment="Instructores con country normalizado"
)
def silver_instructors_dlt():
    """Rellena country NULL con 'Desconocido'"""
    return (dlt.read_stream("bronze_instructors_dlt")
        .fillna({"country": "Desconocido"}))

# ════════════════════════════════════════════════════════════════════════════════
# 🥇 GOLD — Métricas de negocio
# ════════════════════════════════════════════════════════════════════════════════

@dlt.table(
    name="gold_completion_rate_dlt",
    comment="% completitud por curso - Materializada"
)
def gold_completion_rate_dlt():
    """Porcentaje de usuarios que completaron 100% del curso"""
    return (dlt.read("silver_progress_dlt")
        .join(dlt.read("silver_courses_dlt").select("course_id","title"), "course_id", "left")
        .groupBy("course_id","title")
        .agg(
            count("user_id").alias("total_usuarios"),
            round(avg(col("lessons_completed") / col("total_lessons") * 100), 2).alias("completion_pct")
        ))

@dlt.table(
    name="gold_revenue_instructor_dlt",
    comment="Revenue total por instructor - Materializada"
)
def gold_revenue_instructor_dlt():
    """Ingresos acumulados y número de inscripciones por instructor"""
    return (dlt.read("silver_enrollments_dlt")
        .join(dlt.read("silver_courses_dlt").select("course_id","instructor_id"), "course_id", "left")
        .join(dlt.read("silver_instructors_dlt").select("instructor_id","name","country"), "instructor_id", "left")
        .groupBy("instructor_id","name","country")
        .agg(
            round(sum(col("payment_amount")), 2).alias("total_revenue_usd"),
            count("enrollment_id").alias("total_inscripciones")
        )
        .orderBy("total_revenue_usd", ascending=False))

@dlt.table(
    name="gold_cursos_abandonados_dlt",
    comment="Cursos con baja completitud - Materializada"
)
def gold_cursos_abandonados_dlt():
    """Cursos con alta inscripción pero bajo completion rate"""
    completion = dlt.read("gold_completion_rate_dlt")
    inscritos = (dlt.read("silver_enrollments_dlt")
        .groupBy("course_id")
        .agg(count("enrollment_id").alias("total_inscritos")))
    
    return (completion
        .join(inscritos, "course_id", "left")
        .orderBy("completion_pct", ascending=True))

@dlt.table(
    name="gold_top_categorias_dlt",
    comment="Revenue por categoría - Materializada"
)
def gold_top_categorias_dlt():
    """Categorías que más ingresos generan"""
    return (dlt.read("silver_enrollments_dlt")
        .join(dlt.read("silver_courses_dlt").select("course_id","category"), "course_id", "left")
        .groupBy("category")
        .agg(
            round(sum(col("payment_amount")), 2).alias("revenue_usd"),
            count("enrollment_id").alias("inscripciones")
        )
        .orderBy("revenue_usd", ascending=False))
