#!/usr/bin/env bash

export WORK_DIR=$(cd ..;pwd)
export BASE_DIR=/vagrant
export PYTHONPATH=${BASE_DIR}:$PYTHONPATH

query_date=$1

python3 ${WORK_DIR}/time_line_chart_data_generator.py \
    --config=${WORK_DIR}/conf/chart_data.schema.json \
    --date=${query_date} \
    --print \
    --output-file=${WORK_DIR}/output/${query_date}.data.json