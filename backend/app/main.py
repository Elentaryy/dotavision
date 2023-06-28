from fastapi import FastAPI
import logging 
from time import sleep
from api.routers import live_router, history_router, match_router

logging.basicConfig(
    filename='app.log',
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

app = FastAPI()

app.include_router(live_router.router)
app.include_router(history_router.router)
app.include_router(match_router.router)

@app.get("/")
def read_root():
    return {"dota": "test"}

