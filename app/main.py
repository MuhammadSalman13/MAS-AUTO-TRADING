from fastapi import FastAPI
from app.schemas import user

app = FastAPI()

@app.get("/")
def read_root():
    return {"message": "MAS Trading Platform is running"}
