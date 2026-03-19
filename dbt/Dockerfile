FROM python:3.11-slim

WORKDIR /dbt

# Install libpq-dev — this provides pg_config that psycopg2 needs
RUN apt-get update && apt-get install -y \
    git \
    curl \
    libpq-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Now dbt-postgres can compile psycopg2 successfully
RUN pip install --no-cache-dir \
    dbt-core==1.8.0 \
    dbt-postgres==1.8.0

# Verify
RUN dbt --version

COPY . .

CMD ["tail", "-f", "/dev/null"]