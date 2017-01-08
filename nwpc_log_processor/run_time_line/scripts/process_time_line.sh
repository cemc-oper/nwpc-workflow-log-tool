#!/usr/bin/env bash

export WORK_DIR=$(cd ..;pwd)
export BASE_DIR=/vagrant
export PYTHONPATH=${BASE_DIR}:$PYTHONPATH

query_date=$1

python ${WORK_DIR}/time_line_processor.py -o nwp_xp -r nwpc_op --date=${query_date} --save-to-db
python ${WORK_DIR}/time_line_processor.py -o nwp_xp -r nwpc_qu --date=${query_date} --save-to-db
python ${WORK_DIR}/time_line_processor.py -o nwp_xp -r eps_nwpc_qu --date=${query_date} --save-to-db
python ${WORK_DIR}/time_line_processor.py -o nwp_xp -r nwpc_pd --date=${query_date} --save-to-db

python ${WORK_DIR}/time_line_chart_data_generator.py \
    --config=${WORK_DIR}/conf/chart_data.schema.json \
    --date=${query_date} \
    --print \
    --output-file=${WORK_DIR}/output/${query_date}.data.json