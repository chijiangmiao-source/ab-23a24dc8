#!/usr/bin/env bash
# One-shot verification entry point for the compose `verify` service.
# Runs build checks, then the test suite, then the HTTP smoke once, and
# reports the overall result via the process exit code.
set -u

cd /app
status=0

echo "== [1/3] build check: byte-compiling app and scripts =="
python -m compileall -q app scripts
if [ $? -ne 0 ]; then
  echo "BUILD CHECK FAILED"
  status=1
fi

if [ "$status" -eq 0 ]; then
  echo "== [2/3] test suite =="
  python -m pytest -q tests
  if [ $? -ne 0 ]; then
    echo "TEST SUITE FAILED"
    status=1
  fi
fi

if [ "$status" -eq 0 ]; then
  echo "== [3/3] HTTP smoke (gap fill, concurrent retries, restart) =="
  python scripts/smoke.py
  if [ $? -ne 0 ]; then
    echo "HTTP SMOKE FAILED"
    status=1
  fi
fi

if [ "$status" -eq 0 ]; then
  echo "VERIFY OK"
else
  echo "VERIFY FAILED"
fi
exit "$status"
