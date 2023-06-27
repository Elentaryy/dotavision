#!/bin/bash

uvicorn main:app --host 0.0.0.0 --port 80 --reload &

sh db_live.sh