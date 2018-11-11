from nwpc_workflow_log_tool_legacy.nwpc_log_collector import collect_log_from_local_file

import datetime
import argparse

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


if __name__ == "__main__":
    start_time = datetime.datetime.now()

    engine = create_engine('mysql+mysqlconnector://windroc:shenyang@10.28.32.175/system-time-line')
    Session = sessionmaker(bind=engine)
    session = Session()

    # argument parser
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description="""\
    DESCRIPTION
        Collect log file.""")

    parser.add_argument("-u", "--user", help="user name", required=True)
    parser.add_argument("-r", "--repo", help="repo name", required=True)
    parser.add_argument("--log-file", help="log file path", required=True)

    args = parser.parse_args()

    user_name = args.user
    repo_name = args.repo
    file_path = args.log_file
    collect_log_from_local_file(user_name, repo_name, file_path, session)

    end_time = datetime.datetime.now()
    print(end_time - start_time)
