#!/usr/bin/env bash

export WORK_DIR=$(cd ..;pwd)
export BASE_DIR=/vagrant
export PYTHONPATH=${BASE_DIR}:$PYTHONPATH

query_date=$1

python3 ${WORK_DIR}/time_line_processor.py -o nwp_xp -r nwpc_op --date=${query_date} --save-to-db
python3 ${WORK_DIR}/time_line_processor.py -o nwp_xp -r nwpc_qu --date=${query_date} --save-to-db
python3 ${WORK_DIR}/time_line_processor.py -o nwp_xp -r eps_nwpc_qu --date=${query_date} --save-to-db
python3 ${WORK_DIR}/time_line_processor.py -o nwp_xp -r nwpc_pd --date=${query_date} --save-to-db
