# phase 1: Development environment build
FROM python:3.9-slim AS dev
ARG BUILDKIT_INLINE_CACHE=1
WORKDIR /app

# install system dependencies
RUN --mount=type=cache,target=/var/cache/apt \
    apt-get update && apt-get install -y curl gcc g++ build-essential

# install Python dependencies
COPY . .
COPY requirements.txt .
RUN --mount=type=cache,target=/root/.cache/pip \
    #pip install --upgrade pip && \
    pip install --user -r requirements.txt

# set environment variables
ENV PATH="/root/.local/bin:${PATH}" \
    FLASK_ENV="development" \
    PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

CMD ["gunicorn", "app:app", "-b", "0.0.0.0:8080", "--workers", "4", "--access-logfile", "-"]

# phase 2: Production environment build
FROM python:3.9-slim AS prod
WORKDIR /app

# copy system dependencies from dev stage
COPY --from=dev /root/.local /root/.local
COPY . .

# set environment variables
ENV PATH="/root/.local/bin:${PATH}" \
    FLASK_ENV="production" \
    PYTHONUNBUFFERED=1

# set workers for Gunicorn
ARG GUNICORN_WORKERS=4
ENV GUNICORN_WORKERS=${GUNICORN_WORKERS}

# set exposed port
EXPOSE 8080


# run command
CMD ["gunicorn", "app:app", "-b", "0.0.0.0:8080", "--workers", "4", "--access-logfile", "-"]
