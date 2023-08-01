from fastapi import FastAPI, BackgroundTasks, APIRouter
import logging 
from api.routers.predictions_router import predictions_router
from api.routers.stats_router import stats_router
from api.routers.match_router import match_router
from services.db_update import update_public_matches, check_live_matches
from services.db_service import db
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
import warnings
warnings.filterwarnings('ignore')

logging.basicConfig(
    filename='app.log',
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

api_router = APIRouter()
api_router.include_router(predictions_router, prefix="/predictions")
api_router.include_router(stats_router, prefix="/stats")
api_router.include_router(match_router, prefix="/match")

app = FastAPI()

app.include_router(api_router, prefix="/api")

scheduler = BackgroundScheduler()
scheduler.add_job(
        update_public_matches,
        CronTrigger(day_of_week='0', hour=0, minute=0)
    )
scheduler.add_job(
        db.refresh_materialized_views,
        IntervalTrigger(hours=3),
        name='Refresh materialized views every 3 hours', 
        replace_existing=True
    )
scheduler.add_job(
        check_live_matches,
        IntervalTrigger(seconds=20),
        name='Check live matches', 
        replace_existing=True
    )

scheduler.start()

@app.get("/")
def read_root():
    return {"dota": "ggs"}

@app.on_event("shutdown")
def shutdown_event():
    scheduler.shutdown()
