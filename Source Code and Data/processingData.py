from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# 1. Inisialisasi Spark Session
spark = SparkSession.builder \
    .appName("Analyze Crime Data by Year") \
    .config("spark.executor.memory", "2g") \
    .config("spark.driver.memory", "2g") \
    .getOrCreate()

# 2. Memuat Dataset yang Sudah Dibersihkan
cleaned_path = "hdfs:///data/cleaned_crime_reports2.csv"
df_cleaned = spark.read.csv(cleaned_path, header=True, inferSchema=True)

# 3. Hitung jumlah kejahatan per tahun
annual_crimes = df_cleaned.groupBy("Crime Year").count().orderBy("Crime Year")

# 4. Hitung jumlah kejahatan per Neighborhood
neighborhood_crimes = df_cleaned.groupBy("Neighborhood").count().orderBy("count", ascending=False)

# 5. Hitung jumlah kejahatan per Crime Type
crime_type_crimes = df_cleaned.groupBy("Crime").count().orderBy("count", ascending=False)

# 6. Tampilkan hasil
print("Jumlah Kejahatan per Tahun:")
annual_crimes.show()

print("Jumlah Kejahatan per Neighborhood:")
neighborhood_crimes.show()

print("Jumlah Kejahatan per Crime Type:")
crime_type_crimes.show()

# 7. Simpan hasil pengolahan ke HDFS
output_base_path = "hdfs:///output/Crime11"
annual_crimes.write.csv(f"{output_base_path}/annual_crimes.csv", header=True)
neighborhood_crimes.write.csv(f"{output_base_path}/neighborhood_crimes.csv", header=True)
crime_type_crimes.write.csv(f"{output_base_path}/crime_type_crimes.csv", header=True)

# 8. Menutup Spark session
spark.stop()

