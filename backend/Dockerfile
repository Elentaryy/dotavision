FROM python:3.10-slim

WORKDIR /backend/app

COPY ./requirements.txt /backend/requirements.txt

RUN pip install --no-cache-dir -r /backend/requirements.txt

COPY ./app /backend/app

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "80", "--reload"]