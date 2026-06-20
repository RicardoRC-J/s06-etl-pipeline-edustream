"""
Pipeline ETL Medallion — EduStream
Sesión 06 · Bootcamp AI Data Engineer
"""

import pandas as pd
import duckdb
import os
from pyspark.sql.functions import col, when, round as spark_round, avg, sum as spark_sum, count

# ════════════════════════════════════════════════════════════════════════════════
# CONFIGURACIÓN
# ════════════════════════════════════════════════════════════════════════════════

CATALOG = "edustream"
SCHEMA  = "landing"
VOLUME  = "raw"

RUTA_BASE = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"

RUTA_ENROLLMENTS  = f"{RUTA_BASE}/enrollments.csv"
RUTA_COURSES      = f"{RUTA_BASE}/courses.csv"
RUTA_PROGRESS     = f"{RUTA_BASE}/progress.csv"
RUTA_INSTRUCTORS  = f"{RUTA_BASE}/instructors.csv"

print("=" * 80)
print("🎓 PIPELINE MEDALLION — EDUSTREAM")
print("=" * 80)
print(f"\n✅ Rutas configuradas:")
print(f"   {RUTA_ENROLLMENTS}")
print(f"   {RUTA_COURSES}")
print(f"   {RUTA_PROGRESS}")
print(f"   {RUTA_INSTRUCTORS}")

# ════════════════════════════════════════════════════════════════════════════════
# 🥉 BRONZE — Ingesta cruda sin transformar
# ════════════════════════════════════════════════════════════════════════════════

print("\n" + "=" * 80)
print("🥉 PASO 1 — BRONZE: Ingesta cruda")
print("=" * 80)

bronze_enrollments  = spark.read.option("header", True).csv(RUTA_ENROLLMENTS)
bronze_courses      = spark.read.option("header", True).csv(RUTA_COURSES)
bronze_progress     = spark.read.option("header", True).csv(RUTA_PROGRESS)
bronze_instructors  = spark.read.option("header", True).csv(RUTA_INSTRUCTORS)

bronze_enrollments.write.mode("overwrite").saveAsTable("edustream.landing.bronze_enrollments")
bronze_courses.write.mode("overwrite").saveAsTable("edustream.landing.bronze_courses")
bronze_progress.write.mode("overwrite").saveAsTable("edustream.landing.bronze_progress")
bronze_instructors.write.mode("overwrite").saveAsTable("edustream.landing.bronze_instructors")

print(f"✅ bronze_enrollments:  {bronze_enrollments.count()} filas")
print(f"✅ bronze_courses:      {bronze_courses.count()} filas")
print(f"✅ bronze_progress:     {bronze_progress.count()} filas")
print(f"✅ bronze_instructors:  {bronze_instructors.count()} filas")

# ════════════════════════════════════════════════════════════════════════════════
# 🥈 SILVER — Limpieza, normalización y joins
# ════════════════════════════════════════════════════════════════════════════════

print("\n" + "=" * 80)
print("🥈 PASO 2 — SILVER: Limpieza y transformación")
print("=" * 80)

# SILVER INSTRUCTORS
silver_instructors = bronze_instructors.fillna({"country": "Desconocido"})
silver_instructors.write.mode("overwrite").saveAsTable("edustream.landing.silver_instructors")
print(f"✅ silver_instructors: {silver_instructors.count()} filas")

# SILVER COURSES
silver_courses = bronze_courses.withColumn("category",
    when((col("category") == "") | col("category").isNull(), "Sin categoría")
    .otherwise(col("category")))
silver_courses.write.mode("overwrite").saveAsTable("edustream.landing.silver_courses")
print(f"✅ silver_courses: {silver_courses.count()} filas")

# SILVER PROGRESS — Filtra corruptos
silver_progress = bronze_progress.filter(col("total_lessons") > 0)
silver_progress.write.mode("overwrite").saveAsTable("edustream.landing.silver_progress")
print(f"✅ silver_progress: {silver_progress.count()} filas (corruptos eliminados)")

# SILVER ENROLLMENTS — Normaliza moneda a USD
silver_enrollments = bronze_enrollments.withColumn("payment_usd",
    when(col("currency") == "MXN", spark_round(col("payment_amount") * 0.058, 2))
    .when(col("currency") == "COP", spark_round(col("payment_amount") * 0.00024, 2))
    .when(col("currency") == "PEN", spark_round(col("payment_amount") * 0.27, 2))
    .otherwise(spark_round(col("payment_amount"), 2)))
silver_enrollments.write.mode("overwrite").saveAsTable("edustream.landing.silver_enrollments")
print(f"✅ silver_enrollments: {silver_enrollments.count()} filas")

# ════════════════════════════════════════════════════════════════════════════════
# 🥇 GOLD — Métricas de negocio
# ════════════════════════════════════════════════════════════════════════════════

print("\n" + "=" * 80)
print("🥇 PASO 3 — GOLD: Métricas de negocio")
print("=" * 80)

# Convertir tipos de datos para cálculos
silver_progress_pct = silver_progress \
    .withColumn("lessons_completed", col("lessons_completed").cast("double")) \
    .withColumn("total_lessons", col("total_lessons").cast("double")) \
    .withColumn("completion_pct",
        spark_round((col("lessons_completed") / col("total_lessons")) * 100, 2))

silver_enrollments2 = silver_enrollments \
    .withColumn("payment_usd", col("payment_usd").cast("double"))

# GOLD 1 — Completion rate
gold_completion = (
    silver_progress_pct
    .join(silver_courses.select("course_id","title","category"), "course_id", "left")
    .groupBy("course_id","title","category")
    .agg(
        count("user_id").alias("total_usuarios"),
        spark_round(avg("completion_pct"), 2).alias("avg_completion_pct")
    )
)
gold_completion.write.mode("overwrite").saveAsTable("edustream.landing.gold_completion_rate")
print(f"✅ gold_completion_rate: {gold_completion.count()} cursos")

# GOLD 2 — Revenue por instructor
gold_revenue = (
    silver_enrollments2
    .join(silver_courses.select("course_id","instructor_id"), "course_id", "left")
    .join(silver_instructors.select("instructor_id","name","country"), "instructor_id", "left")
    .groupBy("instructor_id","name","country")
    .agg(
        spark_round(spark_sum("payment_usd"), 2).alias("total_revenue_usd"),
        count("enrollment_id").alias("total_inscripciones")
    )
    .orderBy("total_revenue_usd", ascending=False)
)
gold_revenue.write.mode("overwrite").saveAsTable("edustream.landing.gold_revenue_instructor")
print(f"✅ gold_revenue_instructor: {gold_revenue.count()} instructores")

# GOLD 3 — Cursos abandonados
inscritos_por_curso = silver_enrollments2.groupBy("course_id").agg(
    count("enrollment_id").alias("inscritos")
)
gold_abandonados = (
    gold_completion
    .join(inscritos_por_curso, "course_id", "left")
    .orderBy("avg_completion_pct", ascending=True)
)
gold_abandonados.write.mode("overwrite").saveAsTable("edustream.landing.gold_cursos_abandonados")
print(f"✅ gold_cursos_abandonados: {gold_abandonados.count()} cursos")

# GOLD 4 — Top categorías
gold_categorias = (
    silver_enrollments2
    .join(silver_courses.select("course_id","category"), "course_id", "left")
    .groupBy("category")
    .agg(
        spark_round(spark_sum("payment_usd"), 2).alias("revenue_usd"),
        count("enrollment_id").alias("inscripciones")
    )
    .orderBy("revenue_usd", ascending=False)
)
gold_categorias.write.mode("overwrite").saveAsTable("edustream.landing.gold_top_categorias")
print(f"✅ gold_top_categorias: {gold_categorias.count()} categorías")

# ════════════════════════════════════════════════════════════════════════════════
# 📊 RESUMEN FINAL
# ════════════════════════════════════════════════════════════════════════════════

print("\n" + "=" * 80)
print("✅ PIPELINE MEDALLION COMPLETADO")
print("=" * 80)

print("\n🥉 BRONZE:")
print(f"   bronze_enrollments:  {bronze_enrollments.count()} filas")
print(f"   bronze_courses:      {bronze_courses.count()} filas")
print(f"   bronze_progress:     {bronze_progress.count()} filas")
print(f"   bronze_instructors:  {bronze_instructors.count()} filas")

print("\n🥈 SILVER:")
print(f"   silver_enrollments:  {silver_enrollments.count()} filas")
print(f"   silver_courses:      {silver_courses.count()} filas")
print(f"   silver_progress:     {silver_progress.count()} filas")
print(f"   silver_instructors:  {silver_instructors.count()} filas")

print("\n🥇 GOLD:")
print(f"   gold_completion_rate:    {gold_completion.count()} cursos")
print(f"   gold_revenue_instructor: {gold_revenue.count()} instructores")
print(f"   gold_cursos_abandonados: {gold_abandonados.count()} cursos")
print(f"   gold_top_categorias:     {gold_categorias.count()} categorías")

print("\n" + "=" * 80)
