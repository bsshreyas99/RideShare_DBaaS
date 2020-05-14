FROM python:3.6

WORKDIR /app

COPY requirements.txt /app
RUN pip install -r requirements.txt

COPY worker.py /app
CMD ["python3","worker.py"]
