FROM python:3.12-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY broker.py .

EXPOSE 5672
CMD ["python", "broker.py", "--host", "0.0.0.0", "--port", "5672"]
