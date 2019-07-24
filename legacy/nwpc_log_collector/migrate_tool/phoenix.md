# Phoenix

create table


```sql
CREATE TABLE RECORD_NWPC_PD (
  repo_id INTEGER NOT NULL,
  version_id INTEGER NOT NULL,
  line_no INTEGER NOT NULL,
  record_type VARCHAR(100) DEFAULT NULL,
  record_datetime DATE DEFAULT NULL,
  record_command VARCHAR(100) DEFAULT NULL,
  record_fullname VARCHAR(200) DEFAULT NULL,
  record_additional_information VARCHAR,
  record_string VARCHAR,
  CONSTRAINT record_line_no_index PRIMARY KEY (repo_id,version_id,line_no));
```

create index


```sql
CREATE INDEX record_datetime_index ON record_nwpc_pd (record_datetime) ASYNC;
```

import csv

```bash
$PHOENIX_ROOT/bin/psql.py -t RECORD_NWPC_PD localhost nwpc_pd.csv 
```