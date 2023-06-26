from fastapi import FastAPI
import logging 
from services.db_update import check_live_matches
from time import sleep

logging.basicConfig(
    filename='app.log',
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

app = FastAPI()
check_live_matches()

#check_live_matches()
@app.get("/")
def read_root():
    return {"dota": "test"}