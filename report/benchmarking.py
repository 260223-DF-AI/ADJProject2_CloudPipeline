import os
from dotenv import load_dotenv
load_dotenv()



sales_csv1_size = os.path.getsize(os.getenv("SALES_CSV1"))
sales_csv2_size = os.path.getsize(os.getenv("SALES_CSV2"))
sales_csv3_size = os.path.getsize(os.getenv("SALES_CSV3"))
sales_csv4_size = os.path.getsize(os.getenv("SALES_CSV4"))
sales_csv5_size = os.path.getsize(os.getenv("SALES_CSV5"))
total_csv_size = sales_csv1_size + sales_csv2_size + sales_csv3_size + sales_csv4_size + sales_csv5_size
# print(f"Total CSV file size: {(total_csv_size / (1024 * 1024)):.2f} MB")

parquet_file_size = os.path.getsize(os.getenv("PARQUET_FILE"))
# print(f"Parquet file size: {(parquet_file_size / (1024 * 1024)):.2f} MB")

# print(f"File savings %: {((total_csv_size - parquet_file_size) / total_csv_size) * 100:.2f}%")

# Total CSV file size: 182.18 MB
# Parquet file size: 43.50 MB
# File savings %: 76.12%


fileObj = open("/Users/mehta/Desktop/Revature/RevatureGitHubFiles/ADJProject2_CloudPipeline/report/benchmark.txt", "w")
fileObj.write(f"Total CSV file size: {(total_csv_size / (1024 * 1024)):.2f} MB\n")
fileObj.write(f"Parquet file size: {(parquet_file_size / (1024 * 1024)):.2f} MB\n")
fileObj.write(f"File savings %: {((total_csv_size - parquet_file_size) / total_csv_size) * 100:.2f}%\n")
fileObj.close()