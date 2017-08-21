# coding: utf-8
import datetime

import phoenixdb


from nwpc_log_model.rdbms_model import Record
from nwpc_log_model.util.version_util import get_version, session


def mysql_to_phoenixdb(user_name, repo_name):
    connection = phoenixdb.connect('10.28.32.114:8765', autocommit=True)
    print(connection)

    cursor = connection.cursor()

    Record.prepare(user_name, repo_name)
    query = session.query(Record).filter(Record.record_date == datetime.date(2017, 8, 6)) \
        .order_by(Record.record_date, Record.record_time, Record.version_id, Record.line_no)

    for record in query.yield_per(100):
        print(record.record_command, record.record_fullname)
        record_date = record.record_date
        record_time = record.record_time

        sql = ("UPSERT INTO RECORD_NWP_PD ("
               "repo_id, version_id, line_no, record_type, record_date, record_time, record_command,"
               "record_fullname, record_additional_information, record_string) "
               "VALUES ("
               "?, ?, ?, ?, ?, "
               "?, ?,"
               "?, ?, ?)")

        # print(sql)

        cursor.execute(sql, [
            record.repo_id,
            record.version_id,
            record.line_no,
            record.record_type,
            record_date,
            record_time,
            record.record_command,
            record.record_fullname,
            record.record_additional_information,
            record.record_string
        ])


if __name__ == "__main__":
    mysql_to_phoenixdb('nwp_xp', 'nwpc_pd')
