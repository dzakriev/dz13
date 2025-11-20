FROM apache/spark:latest

WORKDIR /app
COPY app.py /app/app.py

CMD ["/opt/spark/bin/spark-submit", "/app/app.py"]
