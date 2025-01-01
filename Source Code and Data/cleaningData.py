from pyspark.sql import SparkSession
from pyspark.sql.functions import split, regexp_extract, col

# 1. Inisialisasi Spark Session
spark = SparkSession.builder \
    .appName("Crime Data Cleaning") \
    .config("spark.executor.memory", "2g") \
    .config("spark.driver.memory", "2g") \
    .getOrCreate()

# 2. Memuat Dataset dari HDFS
hdfs_path = "hdfs:///data/crime_reports/crime_reports.csv"  # Path dataset asli
df = spark.read.csv(hdfs_path, header=True, inferSchema=True)

# Data sebelum pembersihan
print(f"Jumlah baris sebelum pembersihan: {df.count()}")

# 3. Menghapus baris dengan nilai "Admin Error" di kolom 'Crime', 'Neighborhood', atau 'Location'
df_clean = df.filter(~(
    (df['Crime'] == 'Admin Error') |
    (df['Neighborhood'] == 'Admin Error') |
    (df['Location'] == 'Admin Error')
))

# 4. Mengganti nilai kosong (None) dengan "NA" di semua kolom
df_clean = df_clean.fillna("NA")

# 5. Ekstrak tahun dari kolom 'Crime Date Time'
# Menggunakan regex untuk menangkap tahun pertama dari kolom
df_clean = df_clean.withColumn(
    "Crime Year",
    regexp_extract(col("Crime Date Time"), r"(\d{4})", 1)  # Ambil tahun pertama yang ditemukan
)

# Verifikasi data setelah pembersihan
print(f"Jumlah baris setelah pembersihan: {df_clean.count()}")
df_clean.show(5)

# 6. Simpan dataset bersih ke HDFS dalam satu file
output_clean_path = "hdfs:///data/cleaned_crime_reports2.csv"  # Path dataset bersih
df_clean.coalesce(1).write.csv(output_clean_path, header=True)

# 7. Menutup Spark session
spark.stop()

