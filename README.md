# nwpc-workflow-log-tool

A log tool for workflow systems in NWPC. Support ecFlow logs ( SMS support is legacy).

## Installation

The following required packages should be installed manually:

- [nwpc-workflow-model](https://github.com/nwpc-oper/nwpc-workflow-model)
- [nwpc-workflow-log-model](https://github.com/nwpc-oper/nwpc-workflow-log-model)
- [nwpc-workflow-log-collector](https://github.com/nwpc-oper/nwpc-workflow-log-collector)

## Getting started

Run `python -m nwpc_workflow_log_tool` to use command line tool.

### Time period

Show running period for GRAPES MESO 3km model task from 2020-03-01 to 2020-03-19.

```shell script
python -m nwpc_workflow_log_tool node \
    time-period \
    --log-file /g1/u/nwp_qu/ecfworks/ecflow/login_b01.31067.ecf.log \
    --node-path=/grapes_meso_3km_v4_4/00/model/fcst \
    --node-type=task \
    --start-date=2020-03-01 \
    --stop-date=2020-03-20
```

### Time point

Show time point when a node enters some state.
For example `complete` means task is finished, and `submitted` means task starts to run.

Show time point for GRAPES GFS upload task from 2019-02-01 to 2020-01-01

```shell script
python -m nwpc_workflow_log_tool node \
    time-point \
    --log-file /g1/u/nwp_pd/ecfworks/ecflow/login_b01.31071.ecf.log \
    --node-path /gmf_grapes_gfs_post/18/upload/ftp_togrib2/upload_togrib2_global \
    --node-status complete \
    --node-type family \
    --start-date 2019-02-01 \
    --stop-date 2020-01-01
```

### Speed up

In order to speed up the operation, it is recommended to use `grep` to extract required lines from ecflow log.

For example, the following command extracts log entries related to GRAPES MESO 3km model task from ecflow log.

```shell script
grep -P "/grapes_meso_3km_v4_4/(00|06|12|18)/model/fcst(?!_)" \
    "/g1/u/nwp_qu/ecfworks/ecflow/login_b01.31067.ecf.log" \
    > fcst.txt
```

More examples are under `example` directory.

## LICENSE

Copyright &copy; 2017-2020, perillaroc at nwpc-oper.

`nwpc-workflow-log-tool` is licensed under [GPL v3.0](LICENSE.md)