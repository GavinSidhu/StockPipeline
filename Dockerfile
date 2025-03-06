FROM python:3.10-slim

#Rust and Cargo dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    && curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y \
    && apt-get clean \
    && rm- -rf /var/lib/apt/lists/*

ENV PATH="/root/.cargo/bin:${PATH}"

WORKDIR /app

COPY requirements.txt
RUN pip install -r requirements.txt

COPY . .

ENV DAGSTER_HOME=/opt/dagster/DAGSTER_HOME
RUN mkdir -p $DAGSTER_HOME

COPY dagster_home/dagster.yaml $DAGSTER_HOME/

EXPOSE 3000

CMD ["dagster", "dev", "-f", "stock_etl/definitions.py"]