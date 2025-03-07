FROM python:3-slim-bookworm
WORKDIR /steam-pipeline

RUN apt-get update && apt-get -y upgrade && apt-get install -y gcc
RUN pip install --upgrade pip

COPY . .
RUN pip install -r requirements.txt

CMD ["python", "./steam-pipeline.py"]