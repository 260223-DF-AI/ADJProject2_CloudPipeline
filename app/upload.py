"""Script parses our CSV files in chunks, transforms to one large parquet and then uploads to GCS"""
# from google.cloud import storage
# from google.cloud import bigquery
# from google.oauth2 import service_account
import pandas as pd
from io import BytesIO
import pyarrow as pa
import pyarrow.parquet as pq
from dotenv import load_dotenv

import os


load_dotenv()

file_paths = [os.getenv("FILEPATH_ONE"), os.getenv("FILEPATH_TWO"), os.getenv("FILEPATH_THREE"), os.getenv("FILEPATH_FOUR")]
OUTPUT_FILE = os.getenv("PARQUET_FILE") # one big parquet file
CHUNK_SIZE = 50000 # 50,000 rows chunking at a time


def csv_to_parquet(csv_file_paths, output_file_path):
    first_chunk = True
    parquet_writer = None # dedicated object to write to parquet files
    for csv_file in csv_file_paths:
        # access each csv file
        print("processing file:", csv_file)
        for chunk in pd.read_csv(csv_file, chunksize=CHUNK_SIZE):
            # chunk in one chunk of CSV data and convert to PyArrow Table
            table = pa.Table.from_pandas(chunk)

            # make parquet writer if this is first_chunk
            if first_chunk:
                parquet_writer = pq.ParquetWriter(output_file_path, table.schema)
                first_chunk = False

            parquet_writer.write_table(table)# write to our parqyet

    parquet_writer.close()
    print("parquet file made")
    return 0





csv_to_parquet(file_paths, OUTPUT_FILE)

# print(f"sales-data.parquet mb size: {os.path.getsize("/Users/mehta/Desktop/Revature/RevatureGitHubFiles/ADJProject2_CloudPipeline/data/sales-data.parquet") / 1024 / 1024}")