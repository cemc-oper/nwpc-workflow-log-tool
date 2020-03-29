# nwpc-workflow-log-tool

A log tool for workflow systems in NWPC.
Support ecFlow and SMS logs.

## Getting started

Collect from log files and store in MySQL using
[nwpc-workflow-log-collector](https://github.com/nwpc-oper/nwpc-workflow-log-collector).

Process log records from log files or MySQL using processors
in [nwpc-workflow-log-processor](https://github.com/nwpc-oper/nwpc-workflow-log-processor).

Generate time line chart using complied `time_line_chart_tool.js` in `nwpc-workflow-log-chart`.

## LICENSE

Copyright &copy; 2017-2020, perillaroc at nwpc-oper.

`nwpc-workflow-log-tool` is licensed under [GPL v3.0](LICENSE.md)