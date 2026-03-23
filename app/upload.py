"""Script parses our CSV files in chunks, transforms to one large parquet and then uploads to GCS"""
from google.cloud import storage
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
from io import BytesIO
import pyarrow as pa
import pyarrow.parquet as pq
from dotenv import load_dotenv

import os
import logging

load_dotenv()

# implementing logger functionality
logger = logging.getLogger(__name__)
console_handler = logging.StreamHandler()
file_handler = logging.FileHandler('message.log')
fomatter = logging.Formatter('%(asctime)s | %(levelname)s | %(message)s')

logger.setLevel(logging.INFO)

console_handler.setFormatter(fomatter)
file_handler.setFormatter(fomatter)

logger.addHandler(console_handler)
logger.addHandler(file_handler)


#FILE_PATHS = [os.getenv("SALES_CSV1"), os.getenv("SALES_CSV2"), os.getenv("SALES_CSV3"), os.getenv("SALES_CSV4")]
#OUTPUT_FILE = os.getenv("PARQUET_FILE") # one big parquet file
#CHUNK_SIZE = 50000 # 50,000 rows chunking at a time

def csv_to_parquet(csvFilePaths: list, outputFilePaths: str):
    CHUNK_SIZE = 50000
    first_chunk = True
    parquet_writer = None # dedicated object to write to parquet files
    for csv_file in csvFilePaths:
        # access each csv file
        # print("processing file:", csv_file)
        try:
            for chunk in pd.read_csv(csv_file, chunksize=CHUNK_SIZE):
                logger.info(f"Processing and reading file: {csv_file}")
                # chunk in one chunk of CSV data and convert to PyArrow Table
                table = pa.Table.from_pandas(chunk)
                # make parquet writer if this is first_chunk
                if first_chunk:
                    parquet_writer = pq.ParquetWriter(outputFilePaths, table.schema)
                    first_chunk = False
                logger.info(f"Writing to parquet file: {outputFilePaths}")
                parquet_writer.write_table(table)# write to our parqyet
        except pd.errors.ParserError as e:
            logger.warning(f"Error parsing the CSV file. Error message: {e}")
            raise RuntimeError("Something went wrong with the parsing, try again e:",e)
        except pq.ArrowIOError as e:
            logger.warning(f"I/O error: {e}")
            raise RuntimeError(f"PyArrow I/O error: {e}")
        except FileNotFoundError as e:
            logger.warning(f"File not found. Error message: {e}")
            raise FileNotFoundError("Filepath wrong e: ",e)
        except Exception as e:
            logger.error(f"An unexpected error occurred. Error message: {e}")
            exit(-1) # quit now, we need to fix this

    parquet_writer.close()
    print("parquet file made")
    return 0


def parquet_to_gcs(parquetFile: str):
    logger.info(f"Uploading {parquetFile} to GCS")
    bucket_name = os.getenv("BUCKET_NAME")
    # make client and bucket
    client = storage.Client()
    bucket = client.bucket(bucket_name)

    # get base file name from parquet file and not the entire path
    file_name = os.path.basename(parquetFile)
    blob = bucket.blob(file_name)

    blob.upload_from_filename(parquetFile)

    logger.info(f"Uploaded {parquetFile} to gs://{bucket_name}/{file_name}")



if __name__ == "__main__":
    OUTPUT_FILE = os.getenv("PARQUET_FILE")
    # csv_to_parquet(FILE_PATHS, OUTPUT_FILE)
    parquet_to_gcs(OUTPUT_FILE)

# print(f"sales-data.parquet mb size: {os.path.getsize("/Users/mehta/Desktop/Revature/RevatureGitHubFiles/ADJProject2_CloudPipeline/data/sales-data.parquet") / 1024 / 1024}")
