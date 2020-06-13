# nwpc-workflow-log-tool

A log tool for workflow systems in [NWPC/CMA](http://nwpc.nmc.cn/). 
Support ecFlow logs ( SMS support is legacy).

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
    --start-date=2020-06-01 \
    --stop-date=2020-06-11
```

Output:

```
           start_time start_clock end_clock duration
2020060100 2020-06-01    04:36:51  05:39:09 01:02:18
2020060200 2020-06-02    04:37:33  05:47:03 01:09:30
2020060300 2020-06-03    04:38:30  05:40:24 01:01:54
2020060400 2020-06-04    04:38:01  05:50:08 01:12:07
2020060500 2020-06-05    04:37:40  05:48:27 01:10:47
2020060600 2020-06-06    04:37:29  05:43:24 01:05:55
2020060700 2020-06-07    04:37:40  05:43:01 01:05:21
2020060800 2020-06-08    04:38:29  05:49:52 01:11:23
2020060900 2020-06-09    04:37:35  05:39:51 01:02:16
2020061000 2020-06-10    04:37:28  05:42:19 01:04:51

Trimmed Mean for start time (0.25):
0 days 04:37:39.666666

Trimmed Mean for end time (0.25):
0 days 05:44:06.333333
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
    --start-date 2020-06-01 \
    --stop-date 2020-06-11
```

Output

```
2020060100   22:39:44
2020060200   23:04:29
2020060300   22:37:49
2020060400   22:40:37
2020060500   22:53:59
2020060600   22:41:17
2020060700   22:47:47
2020060800   22:43:02
2020060900   22:40:50
2020061000   22:41:33
dtype: timedelta64[ns]

Mean:
0 days 22:45:06.700000

Trim Mean (0.25):
0 days 22:42:31
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