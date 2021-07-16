FROM python:3.9-slim-buster

WORKDIR /oeh_data_dashboard
COPY ./ ./
RUN pip install -r requirements.txt
EXPOSE 8050
EXPOSE 9200
WORKDIR /
RUN ls

CMD [ "python", "-m", "oeh_data_dashboard"]