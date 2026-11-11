#!/bin/sh
set -e
python -m app.db.wait_for_migrations
exec "$@"

