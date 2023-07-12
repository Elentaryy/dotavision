from fastapi import FastAPI, BackgroundTasks
import logging 
from api.routers.predictions_router import predictions_router
from services.db_update import update_public_matches
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
import warnings
warnings.filterwarnings('ignore')

logging.basicConfig(
    filename='app.log',
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

app = FastAPI()

app.include_router(predictions_router, prefix="/api")
# scheduler = BackgroundScheduler()
# scheduler.add_job(
#         update_public_matches,
#         CronTrigger(day_of_week='0', hour=0, minute=0)
#     )
# scheduler.start()

@app.get("/")
def read_root():
    return {"dota": "ggs"}

# @app.on_event("shutdown")
# def shutdown_event():
#     scheduler.shutdown()
