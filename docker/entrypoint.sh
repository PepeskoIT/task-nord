#!/bin/sh

# Run migration
alembic upgrade head

# Run tool
python3 -m main