# coding: utf-8
import datetime
import hashlib

import happybase


from nwpc_log_model.rdbms_model import Record
from nwpc_log_model.util.version_util import get_version, session


def query(user_name, repo_name):
    connection = happybase.Connection('10.28.32.114')
    table = connection.table('record')

    q = table.scan(filter="RowFilter(=, 'regexstring:^.*-59878f51')")
    for key, data in q:
        print(key, data)


if __name__ == "__main__":
    query('nwp_xp', 'nwpc_pd')
