from fastapi import FastAPI
import logging 

logging.basicConfig(
    filename='app.log',
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

app = FastAPI()

@app.get("/")
def read_root():
    return {"dota": "test"}