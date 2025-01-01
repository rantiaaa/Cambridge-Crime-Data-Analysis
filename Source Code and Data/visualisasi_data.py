import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib.offsetbox import AnnotationBbox, TextArea
from pyspark.sql import SparkSession

# Inisialisasi Spark session
spark = SparkSession.builder.appName('CrimeDataAnalysis').getOrCreate()

# Membaca data dari Hadoop/HDFS
annual_crimes = spark.read.csv('hdfs:///output/Crime11/annual_crimes.csv/part-00000-3a975fec-e443-41ef-acd5-bd8fab0d959a-c000.csv', header=True, inferSchema=True)
crime_type_crimes = spark.read.csv('hdfs:///output/Crime11/crime_type_crimes.csv/part-00000-fede7c6c-3f9e-4e67-ab91-33d56e5b77fa-c000.csv', header=True, inferSchema=True)
neighborhood_crimes = spark.read.csv('hdfs:///output/Crime11/neighborhood_crimes.csv/part-00000-54693d34-382a-4527-a12f-47b95293832f-c000.csv', header=True, inferSchema=True)

# Convert Spark DataFrame to pandas DataFrame untuk visualisasi
annual_crimes_df = annual_crimes.toPandas()
crime_type_crimes_df = crime_type_crimes.toPandas()
neighborhood_crimes_df = neighborhood_crimes.toPandas()

# Visualisasi Data Kejahatan Tahunan (Annual Crimes)
plt.figure(figsize=(14, 10))  # Ukuran lebih besar untuk ruang tambahan
sns.lineplot(x='Crime Year', y='count', data=annual_crimes_df, marker='o', color='b', linewidth=2, markersize=8)
plt.title('Tren Kejahatan Tahunan', fontsize=16, fontweight='bold')
plt.xlabel('Tahun', fontsize=12)
plt.ylabel('Jumlah Kejahatan', fontsize=12)
plt.grid(True, linestyle='--', alpha=0.7)
plt.xticks(rotation=45, fontsize=10)

# Menambahkan anotasi di setiap titik
for i, row in annual_crimes_df.iterrows():
    # Menambahkan offset kecil untuk menghindari tumpang tindih
    offset_x = -0.5 if i % 2 == 0 else 0.5  # Bergantian ke kiri dan kanan
    offset_y = 50 if i % 2 == 0 else -50   # Bergantian ke atas dan bawah
    text = f"{row['count']}"
    
    # Menambahkan anotasi teks langsung
    plt.text(row['Crime Year'] + offset_x, row['count'] + offset_y, text, 
             color='red', fontsize=9, weight='bold', ha='center', va='center')

plt.tight_layout()
plt.savefig('annual_crimes_trend_updated.png')  # Menyimpan grafik
plt.close()



# 2. Visualisasi Jenis Kejahatan (Crime Types)
plt.figure(figsize=(12, 8))  # Ukuran lebih besar untuk memberi ruang antar bar
crime_type_crimes_sorted = crime_type_crimes_df.sort_values(by='count', ascending=False)
sns.barplot(x='count', y='Crime', data=crime_type_crimes_sorted, palette='viridis', dodge=False)
plt.title('Jumlah Kejahatan Berdasarkan Jenis')
plt.xlabel('Jumlah Kejahatan')
plt.ylabel('Jenis Kejahatan')
# Menambahkan anotasi pada setiap bar
for i in range(len(crime_type_crimes_sorted)):
    plt.text(crime_type_crimes_sorted['count'].iloc[i] + 5, i, 
             str(crime_type_crimes_sorted['count'].iloc[i]), color='blue', ha="left", va="center", fontsize=10)
plt.tight_layout()
plt.savefig('crime_type_distribution5.png')
plt.close()

# 3. Visualisasi Kejahatan per Neighborhood (Neighborhood Crimes)
plt.figure(figsize=(12, 8))  # Ukuran lebih besar untuk jarak antar bar
neighborhood_crimes_sorted = neighborhood_crimes_df.sort_values(by='count', ascending=False)

# Membuat palette warna berdasarkan nilai `count`
norm = plt.Normalize(neighborhood_crimes_sorted['count'].min(), neighborhood_crimes_sorted['count'].max())
sm = plt.cm.ScalarMappable(cmap='coolwarm', norm=norm)
colors = sm.to_rgba(neighborhood_crimes_sorted['count'])

# Membuat barplot dengan warna berdasarkan nilai
sns.barplot(x='count', y='Neighborhood', data=neighborhood_crimes_sorted, palette=colors)
plt.title('Jumlah Kejahatan Berdasarkan Neighborhood')
plt.xlabel('Jumlah Kejahatan')
plt.ylabel('Neighborhood')

# Menambahkan anotasi pada setiap bar
for i in range(len(neighborhood_crimes_sorted)):
    plt.text(neighborhood_crimes_sorted['count'].iloc[i] + 5, i, 
             str(neighborhood_crimes_sorted['count'].iloc[i]), color='black', ha="left", va="center", fontsize=10)

# Menambahkan colorbar untuk menunjukkan gradien
sm.set_array([])
cbar = plt.colorbar(sm, orientation='vertical', aspect=40)
cbar.set_label('Jumlah Kejahatan')

plt.tight_layout()
plt.savefig('neighborhood_crimes_distribution_gradient5.png')
plt.close()


