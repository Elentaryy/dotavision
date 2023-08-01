FROM python:3.10-slim

WORKDIR /backend/app

COPY ./requirements.txt /backend/requirements.txt

RUN pip install --no-cache-dir -r /backend/requirements.txt

COPY ./app /backend/app

RUN apt-get update && apt-get install -y dos2unix \
    && dos2unix start.sh  \
    && chmod +x start.sh  \
    && apt-get --purge remove -y dos2unix && apt-get -y autoremove

CMD ["sh", "start.sh"]


