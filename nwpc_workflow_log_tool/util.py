from typing import List
import datetime

from nwpc_workflow_log_model.log_record import LogRecord


def generate_in_date_range(start_date, end_date):
    def in_date_range(record):
        return start_date <= record.date <= end_date
    return in_date_range


def generate_later_than_time(current_date: datetime.date, earliest_time: datetime.time):
    d = datetime.datetime.combine(current_date, earliest_time)

    def later_than_time(record: LogRecord):
        record_d = datetime.datetime.combine(record.date, record.time)
        return record_d >= d
    return later_than_time


def print_records(records: List[LogRecord]):
    for r in records:
        print(r.log_record)
