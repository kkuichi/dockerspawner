FROM python:slim

COPY . /src

WORKDIR /src

RUN apt update && \
    apt install -yq --no-install-recommends npm && \
    apt clean && \
    rm -rf /var/lib/apt/lists/* && \
    npm install -g configurable-http-proxy

RUN pip install --upgrade pip && \
    pip install -r ./requirements-dev.txt && \
    pip install .

CMD ["python", "-m", "pytest"]
