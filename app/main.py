from fastapi import FastAPI
from .routers import apiroutes

app = FastAPI(
    title = "Placeholder API",
    description = "Placeholder API",
    version = "0.0.1"
)

app.include_router(apiroutes.router)

@app.get("/")
def get_root():
    return {"message": "Hello from main"}