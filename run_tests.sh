#!/bin/sh
# Run the app_receiver tests in Python 3.13
# PYTHONDONTWRITEBYTECODE and no:cacheprovider stop the root container from leaving files behind.
set -e
cd "$(dirname "$0")"
docker run --rm \
    -v "$PWD/app_receiver:/app" -w /app \
    -e PYTHONDONTWRITEBYTECODE=1 \
    python:3.13-slim \
    sh -c "pip install -q --root-user-action=ignore --disable-pip-version-check -r requirements-dev.txt && pytest -q -p no:cacheprovider $*"
