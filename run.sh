#!/bin/bash
uvicorn app:app --host 127.0.0.1 --port 8000 --reload --log-config log_config.ini
