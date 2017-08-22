# coding: utf-8
import datetime

import phoenixdb


def csv_to_phoenixdb(user_name, repo_name):
    connection = phoenixdb.connect('10.28.32.114:8765', autocommit=True)
    print(connection)


if __name__ == "__main__":
    csv_to_phoenixdb('nwp_xp', 'nwpc_pd')
