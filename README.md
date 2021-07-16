# OEH Data Dashboard

## .env

In the `.env`-file set `ES_HOST="172.17.0.1"` if you run this on a linux machine or to `ES_HOST="host.docker.internal"` if running on a MAC.
Background: The service is expecting a connection to ELASTICSEARCH on the host.
If using docker, we have to connect to this connection from inside the container on the host.
This works differently on Linux and Mac.
So to make this platform agnostic in the script and container, we pass this as an environment variable.

Also look at the `ANALYTICS_INITIAL_COUNT` and `DEBUG` values in the `.env`-file.

## Run app (development)

1. Make sure the port from elasticsearch-instance is forwarded.
1. Set the `ES_HOST`-variable in `.env` file depending on your system (see [.env](#.env)).
1. Create a virtual environment: `python3 -m venv venv`
1. Activate it: `source venv/bin/activate`
1. Install requirements: `pip3 install -r requirements.txt`
1. Run app: `python -m oeh_data_dashboard`


## Run app with Docker (production)

1. Adjust port setting in `docker-compose.yml`.
1. Set `.env`-variables.
1. Run `docker-compose up`.
