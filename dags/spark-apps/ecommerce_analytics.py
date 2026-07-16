"""
Ecommerce Analytics PySpark Script
====================================
Analisis data ecommerce (170.491 baris, 9 kolom) dari MinIO/S3.
Hasil dimuat ke PostgreSQL.

Kolom input:
  event_time, event_type, product_id, category_id, category_code,
  brand, price, user_id, user_session

Analisis:
  1. Funnel / Konversi     → tbl_funnel_analysis
  2. RFM Segmentasi        → tbl_rfm_segments
  3. Brand & Category      → tbl_brand_category_insight, tbl_price_sensitivity
  4. Time-Series           → tbl_hourly_activity, tbl_daily_activity, tbl_daily_revenue

Semua credentials dibaca dari environment variables (K8s Secret).
Tidak ada nilai hardcode di file ini.
"""

import os
import sys
import logging

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, TimestampType
from pyspark.sql.window import Window

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s — %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger('ecommerce_analytics')


# ---------------------------------------------------------------------------
# 1. SparkSession
# ---------------------------------------------------------------------------
def create_spark_session() -> SparkSession:
    """
    Membuat SparkSession dengan konfigurasi:
    - S3A (MinIO) untuk membaca CSV
    - PostgreSQL JDBC (via spark.jars.packages)
    - Credentials dari environment variable (K8s Secret)
    """
    s3_endpoint  = os.environ['S3_ENDPOINT']
    s3_access    = os.environ['S3_ACCESS_KEY']
    s3_secret    = os.environ['S3_SECRET_KEY']

    log.info('Membuat SparkSession...')
    spark = (
        SparkSession.builder
        .appName('EcommerceAnalytics')
        # ── S3A / MinIO Configuration ──────────────────────────────────────
        .config('spark.hadoop.fs.s3a.endpoint',              s3_endpoint)
        .config('spark.hadoop.fs.s3a.access.key',            s3_access)
        .config('spark.hadoop.fs.s3a.secret.key',            s3_secret)
        .config('spark.hadoop.fs.s3a.path.style.access',     'true')
        .config('spark.hadoop.fs.s3a.impl',
                'org.apache.hadoop.fs.s3a.S3AFileSystem')
        .config('spark.hadoop.fs.s3a.connection.ssl.enabled','true')
        # ── Package Dependencies (download dari Maven Central) ─────────────
        # PostgreSQL JDBC + Hadoop-AWS SDK untuk S3A
        .config('spark.jars.packages',
                'org.postgresql:postgresql:42.7.3,'
                'org.apache.hadoop:hadoop-aws:3.3.4,'
                'com.amazonaws:aws-java-sdk-bundle:1.12.262')
        # ── Performance Tuning ─────────────────────────────────────────────
        .config('spark.sql.adaptive.enabled',              'true')
        .config('spark.sql.adaptive.coalescePartitions.enabled', 'true')
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel('WARN')
    log.info(f'SparkSession aktif — App ID: {spark.sparkContext.applicationId}')
    return spark


# ---------------------------------------------------------------------------
# 2. Extract
# ---------------------------------------------------------------------------
def extract_data(spark: SparkSession) -> DataFrame:
    """
    Membaca file CSV ecommerce dari S3/MinIO.
    Path: s3a://<bucket>/<file>
    """
    bucket   = os.environ['S3_BUCKET']
    filename = os.environ['S3_FILE']
    s3_path  = f's3a://{bucket}/{filename}'

    log.info(f'Membaca data dari: {s3_path}')
    df = (
        spark.read
        .option('header', 'true')
        .option('inferSchema', 'true')
        .option('timestampFormat', 'yyyy-MM-dd HH:mm:ss zzz')
        .csv(s3_path)
    )
    total = df.count()
    log.info(f'Data berhasil dimuat: {total:,} baris, {len(df.columns)} kolom')
    return df


# ---------------------------------------------------------------------------
# 3. Transform — Cleaning & Feature Engineering
# ---------------------------------------------------------------------------
def clean_data(df: DataFrame) -> DataFrame:
    """
    - Cast event_time ke TimestampType
    - Tambah kolom waktu: year, month, day_of_week, hour, date
    - Standarisasi nama kolom
    """
    log.info('Membersihkan dan menambahkan kolom waktu...')

    df_clean = (
        df
        .withColumn('event_time',   F.to_timestamp('event_time', 'yyyy-MM-dd HH:mm:ss z'))
        .withColumn('price',        F.col('price').cast(DoubleType()))
        .withColumn('year',         F.year('event_time'))
        .withColumn('month',        F.month('event_time'))
        .withColumn('day_of_week',  F.dayofweek('event_time'))    # 1=Minggu, 7=Sabtu
        .withColumn('hour',         F.hour('event_time'))
        .withColumn('date',         F.to_date('event_time'))
        # Buat label hari yang mudah dibaca
        .withColumn('day_name',
            F.when(F.col('day_of_week') == 1, 'Sunday')
             .when(F.col('day_of_week') == 2, 'Monday')
             .when(F.col('day_of_week') == 3, 'Tuesday')
             .when(F.col('day_of_week') == 4, 'Wednesday')
             .when(F.col('day_of_week') == 5, 'Thursday')
             .when(F.col('day_of_week') == 6, 'Friday')
             .otherwise('Saturday'))
        # Split category_code menjadi kategori utama & sub-kategori
        .withColumn('category_main',
            F.split(F.col('category_code'), r'\.').getItem(0))
        .withColumn('category_sub',
            F.split(F.col('category_code'), r'\.').getItem(1))
    )

    null_category = df_clean.filter(F.col('category_code').isNull()).count()
    null_brand    = df_clean.filter(F.col('brand').isNull()).count()
    log.info(f'Null category_code: {null_category:,} | Null brand: {null_brand:,}')
    return df_clean


# ---------------------------------------------------------------------------
# 4. Analisis 1 — Funnel & Konversi
# ---------------------------------------------------------------------------
def analyze_funnel(df: DataFrame) -> DataFrame:
    """
    Analisis konversi funnel: view → cart → purchase per sesi.

    Kolom output:
      total_sessions, sessions_with_view, sessions_with_cart,
      sessions_with_purchase, view_to_cart_rate, cart_to_purchase_rate,
      overall_conversion_rate

    Dan detail per sesi (untuk drill-down):
      user_session, has_view, has_cart, has_purchase
    """
    log.info('Menghitung Funnel Analysis...')

    # ── Pivot: per sesi, apakah ada view/cart/purchase? ──────────────────
    df_session = (
        df
        .filter(F.col('user_session').isNotNull())
        .groupBy('user_session')
        .agg(
            F.max(F.when(F.col('event_type') == 'view',     1).otherwise(0)).alias('has_view'),
            F.max(F.when(F.col('event_type') == 'cart',     1).otherwise(0)).alias('has_cart'),
            F.max(F.when(F.col('event_type') == 'purchase', 1).otherwise(0)).alias('has_purchase'),
            F.countDistinct('user_id').alias('distinct_users'),
            F.count('*').alias('total_events'),
        )
    )

    # ── Agregasi keseluruhan funnel ───────────────────────────────────────
    df_funnel = df_session.agg(
        F.count('*').alias('total_sessions'),
        F.sum('has_view').alias('sessions_with_view'),
        F.sum('has_cart').alias('sessions_with_cart'),
        F.sum('has_purchase').alias('sessions_with_purchase'),
    ).withColumn(
        'view_to_cart_rate',
        F.round(F.col('sessions_with_cart') / F.col('sessions_with_view') * 100, 2)
    ).withColumn(
        'cart_to_purchase_rate',
        F.round(F.col('sessions_with_purchase') / F.col('sessions_with_cart') * 100, 2)
    ).withColumn(
        'overall_conversion_rate',
        F.round(F.col('sessions_with_purchase') / F.col('total_sessions') * 100, 2)
    )

    df_funnel.show(truncate=False)
    return df_funnel


# ---------------------------------------------------------------------------
# 5. Analisis 2 — RFM Segmentasi Pelanggan
# ---------------------------------------------------------------------------
def analyze_rfm(df: DataFrame) -> DataFrame:
    """
    RFM Segmentasi berdasarkan event_type = 'purchase'.

    Recency  : Hari sejak terakhir purchase (lebih kecil = lebih baik)
    Frequency: Jumlah transaksi purchase per user
    Monetary : Total nilai pembelian per user

    Scoring 1-5 (5 = terbaik) dengan ntile.
    Segmen:
      Champions       : R=5, F=5, M=5
      Loyal           : R>=4, F>=4
      At Risk         : R<=2, F>=3
      Window Shoppers : F=1, M=0 (hanya view, tidak pernah beli)
      Others          : selebihnya
    """
    log.info('Menghitung RFM Segmentation...')

    df_purchase = df.filter(F.col('event_type') == 'purchase')

    # Tanggal referensi = tanggal maksimum dalam dataset + 1 hari
    max_date = df_purchase.agg(F.max('date')).collect()[0][0]
    log.info(f'Tanggal referensi RFM: {max_date}')

    # ── Hitung R, F, M per user ───────────────────────────────────────────
    df_rfm_raw = df_purchase.groupBy('user_id').agg(
        F.datediff(F.lit(max_date), F.max('date')).alias('recency_days'),
        F.count('*').alias('frequency'),
        F.round(F.sum('price'), 2).alias('monetary'),
    )

    # ── Scoring 1-5 menggunakan ntile ─────────────────────────────────────
    # Recency: nilai kecil → skor besar (dibalik)
    w_r = Window.orderBy(F.col('recency_days').desc())
    w_f = Window.orderBy(F.col('frequency').asc())
    w_m = Window.orderBy(F.col('monetary').asc())

    df_scored = (
        df_rfm_raw
        .withColumn('r_score', F.ntile(5).over(w_r))
        .withColumn('f_score', F.ntile(5).over(w_f))
        .withColumn('m_score', F.ntile(5).over(w_m))
        .withColumn('rfm_score',
            F.col('r_score') * 100 + F.col('f_score') * 10 + F.col('m_score'))
    )

    # ── Klasifikasi Segmen ─────────────────────────────────────────────────
    df_segmented = df_scored.withColumn(
        'segment',
        F.when(
            (F.col('r_score') == 5) & (F.col('f_score') == 5) & (F.col('m_score') == 5),
            'Champion'
        ).when(
            (F.col('r_score') >= 4) & (F.col('f_score') >= 4),
            'Loyal Customer'
        ).when(
            (F.col('r_score') >= 3) & (F.col('f_score') >= 3),
            'Potential Loyalist'
        ).when(
            (F.col('r_score') <= 2) & (F.col('f_score') >= 3),
            'At Risk'
        ).when(
            (F.col('r_score') <= 2) & (F.col('f_score') <= 2),
            'Lost'
        ).otherwise('Window Shopper')
    )

    log.info('Distribusi Segmen RFM:')
    df_segmented.groupBy('segment').count().orderBy('count', ascending=False).show()
    return df_segmented


# ---------------------------------------------------------------------------
# 6. Analisis 3A — Brand & Category Insight
# ---------------------------------------------------------------------------
def analyze_brand_category(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """
    Menganalisis performa brand dan kategori:
    - Total views, carts, purchases per brand + kategori
    - Total revenue (hanya purchase events)
    - Hitung conversion rate brand level

    Juga menghitung price sensitivity:
    - Apakah produk mahal lebih banyak di-view tapi jarang dibeli?
    """
    log.info('Menghitung Brand & Category Insight...')

    # Filter null brand & category untuk analisis ini
    df_valid = df.filter(
        F.col('brand').isNotNull() &
        F.col('category_code').isNotNull()
    )

    # ── Performa per Brand + Kategori Utama ──────────────────────────────
    df_brand_cat = df_valid.groupBy('brand', 'category_main').agg(
        F.sum(F.when(F.col('event_type') == 'view',     1).otherwise(0)).alias('total_views'),
        F.sum(F.when(F.col('event_type') == 'cart',     1).otherwise(0)).alias('total_carts'),
        F.sum(F.when(F.col('event_type') == 'purchase', 1).otherwise(0)).alias('total_purchases'),
        F.round(
            F.sum(F.when(F.col('event_type') == 'purchase', F.col('price')).otherwise(0)),
            2
        ).alias('total_revenue'),
        F.round(F.avg('price'), 2).alias('avg_price'),
        F.countDistinct('user_id').alias('unique_users'),
    ).withColumn(
        'view_to_purchase_rate',
        F.round(
            F.when(F.col('total_views') > 0,
                   F.col('total_purchases') / F.col('total_views') * 100
            ).otherwise(0),
            2
        )
    ).orderBy(F.col('total_revenue').desc())

    log.info('Top 10 Brand by Revenue:')
    df_brand_cat.show(10)

    # ── Price Sensitivity Analysis ────────────────────────────────────────
    # Kelompokkan produk ke price bracket, lihat konversi per bracket
    df_price = df_valid.filter(F.col('price') > 0).withColumn(
        'price_bracket',
        F.when(F.col('price') <   50,  'Budget (<$50)')
         .when(F.col('price') <  200,  'Mid ($50-$200)')
         .when(F.col('price') <  500,  'Premium ($200-$500)')
         .when(F.col('price') < 1000,  'Luxury ($500-$1000)')
         .otherwise('Ultra Luxury (>$1000)')
    )

    df_sensitivity = df_price.groupBy('price_bracket').agg(
        F.count('*').alias('total_events'),
        F.sum(F.when(F.col('event_type') == 'view',     1).otherwise(0)).alias('views'),
        F.sum(F.when(F.col('event_type') == 'purchase', 1).otherwise(0)).alias('purchases'),
        F.round(F.avg('price'), 2).alias('avg_price'),
        F.countDistinct('product_id').alias('distinct_products'),
    ).withColumn(
        'purchase_rate_pct',
        F.round(
            F.when(F.col('views') > 0,
                   F.col('purchases') / F.col('views') * 100
            ).otherwise(0),
            2
        )
    ).orderBy('avg_price')

    log.info('Price Sensitivity:')
    df_sensitivity.show()
    return df_brand_cat, df_sensitivity


# ---------------------------------------------------------------------------
# 7. Analisis 4 — Time-Series Activity
# ---------------------------------------------------------------------------
def analyze_timeseries(df: DataFrame) -> tuple[DataFrame, DataFrame, DataFrame]:
    """
    Pola waktu aktivitas pengguna:
    - Per jam (0-23): jumlah event view & purchase
    - Per hari (Senin-Minggu): jumlah event
    - Per tanggal: revenue harian dari purchase events
    """
    log.info('Menghitung Time-Series Activity...')

    # ── Aktivitas per Jam ─────────────────────────────────────────────────
    df_hourly = df.groupBy('hour').agg(
        F.count('*').alias('total_events'),
        F.sum(F.when(F.col('event_type') == 'view',     1).otherwise(0)).alias('views'),
        F.sum(F.when(F.col('event_type') == 'cart',     1).otherwise(0)).alias('carts'),
        F.sum(F.when(F.col('event_type') == 'purchase', 1).otherwise(0)).alias('purchases'),
        F.countDistinct('user_id').alias('unique_users'),
    ).orderBy('hour')

    log.info('Aktivitas per Jam:')
    df_hourly.show(24, truncate=False)

    # ── Aktivitas per Hari ────────────────────────────────────────────────
    df_daily = df.groupBy('day_of_week', 'day_name').agg(
        F.count('*').alias('total_events'),
        F.sum(F.when(F.col('event_type') == 'view',     1).otherwise(0)).alias('views'),
        F.sum(F.when(F.col('event_type') == 'cart',     1).otherwise(0)).alias('carts'),
        F.sum(F.when(F.col('event_type') == 'purchase', 1).otherwise(0)).alias('purchases'),
        F.countDistinct('user_session').alias('unique_sessions'),
    ).orderBy('day_of_week')

    log.info('Aktivitas per Hari:')
    df_daily.show(7, truncate=False)

    # ── Revenue Harian (purchase only) ────────────────────────────────────
    df_daily_revenue = (
        df
        .filter(F.col('event_type') == 'purchase')
        .filter(F.col('price').isNotNull() & (F.col('price') > 0))
        .groupBy('date')
        .agg(
            F.round(F.sum('price'), 2).alias('daily_revenue'),
            F.count('*').alias('total_purchases'),
            F.countDistinct('user_id').alias('unique_buyers'),
            F.round(F.avg('price'), 2).alias('avg_order_value'),
        )
        .orderBy('date')
    )

    log.info('Revenue Harian:')
    df_daily_revenue.show(truncate=False)
    return df_hourly, df_daily, df_daily_revenue


# ---------------------------------------------------------------------------
# 8. Load ke PostgreSQL
# ---------------------------------------------------------------------------
def load_to_postgres(df: DataFrame, table_name: str, mode: str = 'overwrite') -> None:
    """
    Menulis DataFrame ke tabel PostgreSQL via JDBC.
    Default mode='overwrite' agar pipeline idempotent.
    """
    db_host = os.environ['DB_HOST']
    db_port = os.environ['DB_PORT']
    db_name = os.environ['DB_NAME']
    db_user = os.environ['DB_USER']
    db_pass = os.environ['DB_PASSWORD']

    jdbc_url = f'jdbc:postgresql://{db_host}:{db_port}/{db_name}'
    props = {
        'user':     db_user,
        'password': db_pass,
        'driver':   'org.postgresql.Driver',
    }

    row_count = df.count()
    log.info(f'Memuat {row_count:,} baris ke tabel "{table_name}" (mode={mode})...')

    df.write.jdbc(
        url=jdbc_url,
        table=table_name,
        mode=mode,
        properties=props,
    )
    log.info(f'✓ Tabel "{table_name}" berhasil dimuat.')


# ---------------------------------------------------------------------------
# 9. Main Orchestration
# ---------------------------------------------------------------------------
def main():
    log.info('=' * 60)
    log.info('ECOMMERCE ANALYTICS PIPELINE — START')
    log.info('=' * 60)

    # ── Setup ──────────────────────────────────────────────────────────────
    spark = create_spark_session()

    try:
        # ── Extract ────────────────────────────────────────────────────────
        df_raw   = extract_data(spark)
        df_clean = clean_data(df_raw)
        df_clean.cache()   # Cache karena dipakai oleh semua analisis
        log.info(f'Data di-cache, total: {df_clean.count():,} baris')

        # ── Analisis 1: Funnel ─────────────────────────────────────────────
        df_funnel = analyze_funnel(df_clean)
        load_to_postgres(df_funnel, 'tbl_funnel_analysis')

        # ── Analisis 2: RFM ────────────────────────────────────────────────
        df_rfm = analyze_rfm(df_clean)
        load_to_postgres(df_rfm, 'tbl_rfm_segments')

        # ── Analisis 3: Brand & Category ──────────────────────────────────
        df_brand_cat, df_sensitivity = analyze_brand_category(df_clean)
        load_to_postgres(df_brand_cat,   'tbl_brand_category_insight')
        load_to_postgres(df_sensitivity, 'tbl_price_sensitivity')

        # ── Analisis 4: Time-Series ────────────────────────────────────────
        df_hourly, df_daily_act, df_daily_rev = analyze_timeseries(df_clean)
        load_to_postgres(df_hourly,     'tbl_hourly_activity')
        load_to_postgres(df_daily_act,  'tbl_daily_activity')
        load_to_postgres(df_daily_rev,  'tbl_daily_revenue')

        log.info('=' * 60)
        log.info('PIPELINE SELESAI — Semua tabel berhasil dimuat ke PostgreSQL')
        log.info('  ✓ tbl_funnel_analysis')
        log.info('  ✓ tbl_rfm_segments')
        log.info('  ✓ tbl_brand_category_insight')
        log.info('  ✓ tbl_price_sensitivity')
        log.info('  ✓ tbl_hourly_activity')
        log.info('  ✓ tbl_daily_activity')
        log.info('  ✓ tbl_daily_revenue')
        log.info('=' * 60)

    except Exception as e:
        log.error(f'Pipeline gagal: {e}', exc_info=True)
        raise
    finally:
        df_clean.unpersist()
        spark.stop()
        log.info('SparkSession dihentikan.')


if __name__ == '__main__':
    main()
