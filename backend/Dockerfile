FROM python:3.10-slim

WORKDIR /backend/app

COPY ./requirements.txt /backend/requirements.txt

RUN pip install --no-cache-dir -r /backend/requirements.txt

COPY ./app /backend/app

RUN chmod +x start.sh
RUN chmod +x db_live.sh

CMD ["sh", "start.sh"]


