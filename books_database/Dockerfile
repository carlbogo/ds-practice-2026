FROM python:3.9

WORKDIR /app

COPY . .

RUN pip install grpcio grpcio-tools

CMD ["python", "/app/books_database/src/app.py"]