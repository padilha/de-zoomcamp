FROM python:3.9.1

RUN apt-get install wget
RUN pip install pandas==1.5.2 sqlalchemy==1.4.39 pyarrow==8.0.0 psycopg2==2.9.5 psycopg2-binary==2.9.5

WORKDIR /app
COPY ingest_data.py ingest_data.py

ENTRYPOINT ["python", "ingest_data.py"]