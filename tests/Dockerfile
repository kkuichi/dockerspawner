FROM python:slim

COPY . /src

WORKDIR /src

RUN apt-get update && \
    apt-get install -yq --no-install-recommends npm && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* && \
    npm install -g configurable-http-proxy

RUN pip install --upgrade pip && \
    pip install -r ./requirements-dev.txt && \
    pip install .

CMD ["python", "-m", "pytest", "-s"]
