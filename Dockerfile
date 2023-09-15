FROM python:3.10
WORKDIR /code
RUN curl -sSL https://install.python-poetry.org | python3 -
ENV PATH="$PATH:/root/.local/bin"
COPY ./poetry.lock /code/poetry.lock
COPY ./pyproject.toml /code/pyproject.toml
RUN poetry export -f requirements.txt --output requirements.txt --only main
RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt
COPY ./src /code/app
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "3004"]
