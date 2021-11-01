FROM python:3.6-slim

RUN pip3 install flask
RUN pip3 install requests

WORKDIR /app

COPY static /app/static
COPY templates /app/templates
COPY app.py /app/

CMD ["python3", "app.py"]


