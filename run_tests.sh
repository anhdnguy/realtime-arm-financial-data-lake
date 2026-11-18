#!/bin/sh
# Run each app's tests in Python 3.13: app_receiver, then app_watcher.
# One app only: APP=app_watcher ./run_tests.sh tests/test_rules.py
# (extra arguments go to pytest; paths are relative to the app folder).
# PYTHONDONTWRITEBYTECODE and no:cacheprovider stop the root container from leaving files behind.
set -e
cd "$(dirname "$0")"
for app in ${APP:-app_receiver app_watcher}; do
    echo "== $app"
    docker run --rm \
        -v "$PWD/$app:/app" -w /app \
        -e PYTHONDONTWRITEBYTECODE=1 \
        python:3.13-slim \
        sh -c "pip install -q --root-user-action=ignore --disable-pip-version-check -r requirements-dev.txt && pytest -q -p no:cacheprovider $*"
done
