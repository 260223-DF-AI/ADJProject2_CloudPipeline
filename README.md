# ADJProject2_CloudPipeline

This project will read in CSV files and turn them into parquet files for more efficient storage to Google's Cloud Storage (GCS) service. After the parquet files are succesfully uploaded to GCS it will be accessible for SQL queries through Google's BigQuery service. The upload of files to GCS and querying is easily done through our FastAPI implementation that is also connected to a Streamlit app.

## Installation

Use the Python package manager [pip](https://pip.pypa.io/en/stable/) to install the project dependencies.

```bash
pip install -r requirements.txt
```

## Running Instructions (from terminal)
### navigate to ~/ADJProject2_CloudPipeline
**Uvicorn:** (FastAPI)
    
    uvicorn app.main:app --reload  

**Streamlit:** (Interactive App)
    
    streamlit run streamlit-app/streamlit_app.py  


## Running Tests (from terminal)
### navigate to ~/ADJProject2_CloudPipeline
    python3 -m pytest (simple)
    python3 -m pytest -v (detailed)
    python3 -m pytest -v -W ignore (detailed, ignore warnings)

## Technologies Used

- Python
- Google Cloud Storage
- Google Cloud Platform
- BigQuery
- FastAPI
- Uvicorn
- Streamlit

## Contributors
- Andy Mei
- Dio Soetarman
- Jaisal Mehta