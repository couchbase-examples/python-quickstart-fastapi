FROM python:3.13-slim

WORKDIR /code

COPY requirements.txt /code/requirements.txt
RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt

COPY ./app /code/app
COPY ./hotel_search_index.json /code/hotel_search_index.json

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
