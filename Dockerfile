FROM python:3.5

RUN pip install --upgrade pip

WORKDIR /src
COPY . .

RUN pip install -r src/requirements.txt
CMD [ "python", "./src/mongodb_connector.py" ]
