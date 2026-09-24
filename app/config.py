"""Configuration loaded from environment variables."""
from __future__ import annotations

import os


def _dsn() -> str:
    raw = os.environ.get("DATABASE_URL")
    if raw:
        return raw
    return (
        f"postgresql://{os.environ.get('POSTGRES_USER', 'pulse')}:"
        f"{os.environ.get('POSTGRES_PASSWORD', 'pulse')}@"
        f"{os.environ.get('POSTGRES_HOST', 'db')}:"
        f"{os.environ.get('POSTGRES_PORT', '5432')}/"
        f"{os.environ.get('POSTGRES_DB', 'pulse')}"
    )


DATABASE_URL = _dsn()

# A fragment may be staged up to WINDOW_AHEAD shots beyond the current
# committed water mark.
WINDOW_AHEAD = int(os.environ.get("WINDOW_AHEAD", "32"))

# The first legal shot number of a run.
FIRST_SHOT = int(os.environ.get("FIRST_SHOT", "1"))
