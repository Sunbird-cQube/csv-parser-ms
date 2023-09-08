# cQube CSV Parser Service

Parse CSV and generate an ingest folder for users. 

Local setup
-----------

1. Install Poetry
```shell
curl -sSL https://install.python-poetry.org | python3 -
```
2. Install the project
```shell
poetry install
```
3. Run the server
```shell
cp .env.sample .env && cd src && poetry run uvicorn main:app --reload
```
