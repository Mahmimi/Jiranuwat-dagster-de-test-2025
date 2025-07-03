# uv-based Python Image
FROM ghcr.io/astral-sh/uv:0.5.28-python3.12-bookworm@sha256:052754cbb3ffcceb7107e7f2bf022861d3a00398942b7c81bc3868c2f82c51d2

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        curl apt-transport-https gnupg2 ca-certificates unixodbc-dev && \
    curl -sSL https://packages.microsoft.com/keys/microsoft.asc | \
        gpg --dearmor -o /etc/apt/trusted.gpg.d/microsoft.gpg && \
    echo "deb [arch=amd64 signed-by=/etc/apt/trusted.gpg.d/microsoft.gpg] https://packages.microsoft.com/debian/12/prod bookworm main" \
        > /etc/apt/sources.list.d/mssql-release.list && \
    apt-get update && \
    ACCEPT_EULA=Y apt-get install -y msodbcsql18 && \
    apt-get clean && rm -rf /var/lib/apt/lists/*
	
ENV DAGSTER_HOME=/opt/dagster/app
WORKDIR ${DAGSTER_HOME}

ENV UV_PROJECT_ENVIRONMENT=/usr/local

# Install Python dependencies with uv
RUN --mount=type=cache,target=/root/.cache/uv \
	--mount=type=bind,source=uv.lock,target=uv.lock \
	--mount=type=bind,source=pyproject.toml,target=pyproject.toml \
	uv sync --frozen --no-install-project --no-dev

# Copy the project source code and install it
ADD . /opt/dagster/app
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev

EXPOSE 4000
CMD ["dagster", "code-server", "start", "-h", "0.0.0.0", "-p", "4000", "-m", "dagster_pipelines.definitions"]