FROM python:3.6

WORKDIR /app

COPY requirements.txt /app
RUN pip install -r requirements.txt

COPY assignment1.py /app
CMD ["python3","assignment1.py"]