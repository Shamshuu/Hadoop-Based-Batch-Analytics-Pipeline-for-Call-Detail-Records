#!/bin/sh
python3 /data/generate_records.py
# Keep container running to satisfy healthcheck
tail -f /dev/null
