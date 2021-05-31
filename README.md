# OEH Data Dashboard

## .env

In the `.env`-file set `ES_HOST` to `localhost` if you run this on a linux machine or to `host.docker.internal` if running from a MAC.
Background: The service is expecting a connection to ELASTICSEARCH on the host.
If using docker, we have to connect to this connection from inside the container on the host.
This works differently on Linux and Mac. So to make this platform agnostic in the script and container, we pass this as an environment variable.

## Run app (development)

1. Make sure the port from elasticsearch-instance is forwarded.
1. Set the `ES_HOST`-variable in `.env` file to `localhost`.
1. Create a virtual environment: `python3 -m venv venv`
1. Activate it: `source venv/bin/activate`
1. Install requirements: `pip3 install -r requirements.txt`
1. Run app: `python3 app.py`


## Run app (production)

1. Set the `ES_HOST`-variable in `.env` file to `localhost` on Linux or `host.docker.internal` on Mac.
1. Run `docker-compose up`.