FROM python:3.10-slim

WORKDIR /bot/app

COPY ./requirements.txt /bot/requirements.txt

RUN pip install --no-cache-dir -r /bot/requirements.txt

EXPOSE 80

COPY ./app /bot/app

CMD ["python", "-m", "main"]