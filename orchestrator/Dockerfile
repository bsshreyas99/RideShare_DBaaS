FROM python:3.6
EXPOSE 8000
WORKDIR /app

COPY requirements.txt /app
COPY number.txt /app
COPY replica.txt /app

RUN pip install -r requirements.txt

COPY orchestrator.py /app
CMD ["python3","orchestrator.py"]