from fastapi import FastAPI
from .routers import apiroutes
from upload import csv_to_parquet, FILE_PATHS, OUTPUT_FILE
from dotenv import load_dotenv
import os

load_dotenv()

app = FastAPI(
    title = "Placeholder API",
    description = "Placeholder API",
    version = "0.0.1"
)

app.include_router(apiroutes.router)

@app.get("/")
def get_root():
    return {"message": "Hello from main"}


@app.post("/convert")
def post_root():
    pass
    # csv_to_parquet(FILE_PATHS, OUTPUT_FILE)

# run: uvicorn app.main:app --reload