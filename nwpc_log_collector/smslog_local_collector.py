# coding: utf-8
import datetime
import click
import yaml

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from nwpc_log_model.rdbms_model import Record
from nwpc_log_model.util.version_util import VersionUtil
from nwpc_log_util.sms_log_file_util import SmsLogFileUtil


def load_config(config_file_path):
    f = open(config_file_path, 'r')
    config = yaml.load(f)
    f.close()
    return config


def get_session(database_uri):
    engine = create_engine(database_uri)
    Session = sessionmaker(bind=engine)
    session = Session()
    return session


def collect_log_from_local_file(config, user_name, repo_name, file_path):
    session = get_session(config['smslog_local_collector']['rdbms']['database_uri'])

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
            if session_count_to_be_committed >= config['smslog_local_collector']['sms']['post']['max_count']:
                session.commit()
                print('commit session')
                session_count_to_be_committed = 0

        if session_count_to_be_committed > 0:
            session.commit()
            print('commit session')


def collect_log_from_local_file_by_range(config, user_name, repo_name, file_path, start_date, end_date):
    session = get_session(config['smslog_local_collector']['rdbms']['database_uri'])

    with open(file_path) as f:
        first_line = f.readline().strip()
        version = VersionUtil.get_version(user_name, repo_name, file_path, first_line, session)
        Record.prepare(user_name, repo_name)

        print("Finding line no in range:", start_date, end_date)
        begin_line_no, end_line_no = SmsLogFileUtil.get_line_no_range(
            file_path,
            datetime.datetime.strptime(start_date, "%Y-%m-%d").date(),
            datetime.datetime.strptime(end_date, "%Y-%m-%d").date())
        if begin_line_no == 0 or end_line_no == 0:
            click.echo("line not found")
            return
        print("Found line no in range:", begin_line_no, end_line_no)

        for i in range(1, begin_line_no):
            f.readline()

        session_count_to_be_committed = 0
        max_count = config['smslog_local_collector']['sms']['post']['max_count']
        max_count = 1

        cur_line_no = begin_line_no
        for i in range(begin_line_no, end_line_no):
            line = f.readline()
            line = line.strip()
            record = Record()
            print(cur_line_no, line)
            record.parse(line)
            record.repo_id = version.repo_id
            record.version_id = version.version_id
            record.line_no = cur_line_no
            record = session.merge(record)
            cur_line_no += 1

            session_count_to_be_committed += 1
            if session_count_to_be_committed >= max_count:
                session.commit()
                click.echo('commit session')
                session_count_to_be_committed = 0

        if session_count_to_be_committed > 0:
            session.commit()
            click.echo('commit session')


@click.group()
def cli():
    pass


@cli.command()
@click.option('-c', '--config', help='config file path')
@click.option('-o', '--owner', help='owner name')
@click.option('-r', '--repo', help='repo name')
@click.option('-l', '--log-file', help='log file path')
def load(config, owner, repo, log_file):
    config_object = load_config(config)
    collect_log_from_local_file(config_object, owner, repo, log_file)


@cli.command()
@click.option('-c', '--config', help='config file path')
@click.option('-o', '--owner', help='owner name')
@click.option('-r', '--repo', help='repo name')
@click.option('-l', '--log-file', help='log file path')
@click.option('--begin-date', help='begin date, [start_date, end_date), YYYY-MM-dd')
@click.option('--end-date', help='end date, [start_date, end_date), YYYY-MM-dd')
def load_range(config, owner, repo, log_file, begin_date, end_date):
    config_object = load_config(config)
    collect_log_from_local_file_by_range(config_object, owner, repo, log_file, begin_date, end_date)


if __name__ == "__main__":
    cli()
