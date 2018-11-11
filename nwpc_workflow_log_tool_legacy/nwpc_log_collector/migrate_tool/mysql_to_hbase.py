# coding: utf-8
import datetime
import hashlib

import happybase


from nwpc_log_model.rdbms_model import Record
from nwpc_log_model.util.version_util import get_version, session


def mysql_to_hbase(user_name, repo_name):
    connection = happybase.Connection('10.28.32.114')
    print(connection.tables())
    table = connection.table('record')

    Record.prepare(user_name, repo_name)
    query = session.query(Record).filter(Record.record_date == datetime.date(2017, 8, 6)) \
        .order_by(Record.record_date, Record.record_time, Record.version_id, Record.line_no)

    m = hashlib.blake2s(digest_size=4)

    for record in query.yield_per(100):
        print(record.record_command, record.record_fullname)
        record_date = record.record_date
        record_time = record.record_time
        version_id = record.version_id
        line_no = record.line_no
        record_type = record.record_type

        version_line_no_string = "{version_id}.{line_no}".format(
            version_id=version_id,
            line_no=line_no
        )

        m.update(version_line_no_string.encode('utf-8'))
        version_line_no_hash = m.hexdigest()
        record_datetime = datetime.datetime.combine(record_date, record_time)
        record_timestamp = int(record_datetime.timestamp())
        time_string = "{hex_timestamp:08x}".format(hex_timestamp=record_timestamp)

        row_key_string = "{version_line_no_hash}-{time_string}".format(
            version_line_no_hash=version_line_no_hash,
            time_string=time_string
        )

        print(row_key_string.encode('utf-8'))
        table.put(row_key_string, {
            b'r:version_id': str(version_id),
            b'r:line_no': str(line_no),
            b'r:record_type': record_type,
            b'r:record_date': record_datetime.strftime("%Y-%m-%d"),
            b'r:record_time': record_datetime.strftime("%H:%M:%S"),
            b'r:record_command': record.record_command,
            b'r:record_fullname': record.record_fullname,
            b'r:record_additional_information': record.record_additional_information,
            b'r:record_string': record.record_string
        })




if __name__ == "__main__":
    mysql_to_hbase('nwp_xp', 'nwpc_pd')
