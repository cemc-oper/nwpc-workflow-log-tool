# coding: utf-8
import click

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from nwpc_log_model.rdbms_model import Record
from nwpc_log_model.util.version_util import VersionUtil

committed_count = 5000


def collect_log_from_local_file(user_name, repo_name, file_path, session):
    version = None
    with open(file_path) as f:
        first_line = f.readline().strip()
        version = VersionUtil.get_version(user_name, repo_name, file_path, first_line, session)
        Record.prepare(user_name, repo_name)

        query = session.query(Record).filter(Record.repo_id == version.repo_id) \
            .filter(Record.version_id == version.version_id) \
            .order_by(Record.line_no.desc()) \
            .limit(1)

        latest_record = query.first()
        if latest_record is None:
            start_line_no = 0
        else:
            start_line_no = latest_record.line_no + 1

        if start_line_no == 0:
            record = Record()
            record.parse(first_line)
            record.repo_id = version.repo_id
            record.version_id = version.version_id
            record.line_no = 0
            session.add(record)
            start_line_no += 1

        for i in range(1, start_line_no):
            f.readline()

        session_count_to_be_committed = 0

        cur_line_no = start_line_no
        for line in f:
            line = line.strip()
            record = Record()
            print(cur_line_no, line)
            record.parse(line)
            record.repo_id = version.repo_id
            record.version_id = version.version_id
            record.line_no = cur_line_no
            session.add(record)
            cur_line_no += 1

            session_count_to_be_committed += 1
            if session_count_to_be_committed >= committed_count:
                session.commit()
                print('commit session')
                session_count_to_be_committed = 0

        if session_count_to_be_committed > 0:
            session.commit()
            print('commit session')


@click.group()
def cli():
    pass


@cli.command()
@click.option('-o', '--owner', help='owner name')
@click.option('-r', '--repo', help='repo name')
@click.option('-f', '--file-path', help='log file path')
def load(owner, repo, file_path):
    engine = create_engine('mysql+mysqlconnector://windroc:shenyang@10.28.32.175/system-time-line')
    Session = sessionmaker(bind=engine)
    session = Session()
    collect_log_from_local_file(owner, repo, file_path, session)


if __name__ == "__main__":
    cli()