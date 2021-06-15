FROM python:3.9-slim-buster

WORKDIR /code
COPY requirements.txt /
COPY ./ ./
RUN pip install -r /requirements.txt
EXPOSE 8050
EXPOSE 9200

CMD [ "python", "./app.py"]