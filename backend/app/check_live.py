from services.db_update import check_live_matches

import logging 

logging.basicConfig(
    filename='app.log',
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

logger = logging.getLogger('check_live')

if __name__ == '__main__':
    try:
        check_live_matches()
    except Exception as e:
        logger.info(e)
        