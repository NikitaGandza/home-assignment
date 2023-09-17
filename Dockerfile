FROM python:3.9

COPY main.py .
COPY requirements.txt .
COPY .env .

RUN pip install -r requirements.txt



CMD ["python", "./main.py"]
