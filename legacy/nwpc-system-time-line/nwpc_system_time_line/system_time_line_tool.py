# coding=utf-8
import pathlib
import datetime
import subprocess
import json

import click
import yaml

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from nwpc_log_model.rdbms_model.models import Model
from nwpc_log_model.util.repo_util import RepoUtil

from nwpc_log_collector.smslog_local_collector import (
    collect_log_from_local_file_by_range,
    load_config as load_collector_config,
    get_log_info_from_local_file
)
from nwpc_workflow_log_processor.rmdb.run_time_line.time_line_processor import load_processor_config, time_line_processor
from nwpc_workflow_log_processor.rmdb.run_time_line.time_line_chart_data_generator import generate_chart_data


def load_config(config_file_path):
    f = open(config_file_path, 'r')
    config = yaml.load(f)
    f.close()
    return config


@click.group()
def cli():
    """
    NWPC's operation system running time line tool.
    """
    pass


@cli.command()
@click.option('-c', '--config', help='config file path')
@click.option('-o', '--owner', help='owner name')
@click.option('-r', '--repo', help='repo name')
def setup(config, owner, repo):
    """
    set up database.
    """
    config = load_config(config)
    engine = create_engine(config['system_time_line']['rdbms']['database_uri'])
    Session = sessionmaker(bind=engine)
    session = Session()
    Model.metadata.create_all(engine)

    RepoUtil.create_owner(owner, session)
    RepoUtil.create_sms_repo(owner, repo, session)
    RepoUtil.create_record_table(owner, repo, session)


@cli.command()
@click.option('-c', '--config', help='config file path')
@click.option('-o', '--owner', help='owner name')
@click.option('-r', '--repo', help='repo name')
def teardown(config, owner, repo):
    """
    clean up database.
    """
    config = load_config(config)
    engine = create_engine(config['system_time_line']['rdbms']['database_uri'])
    Session = sessionmaker(bind=engine)
    session = Session()
    Model.metadata.drop_all(engine)


@cli.command()
@click.option('-c', '--config', help='config file path')
@click.option('-o', '--owner', help='owner name')
@click.option('-r', '--repo', help='repo name')
@click.option('-l', '--log-file', help='log file path')
@click.option('--begin-date', help='begin date, [start_date, end_date), YYYY-MM-dd')
@click.option('--end-date', help='end date, [start_date, end_date), YYYY-MM-dd')
@click.option('-v', '--verbose', count=True, help='verbose level')
def load(config, owner, repo, log_file, begin_date, end_date, verbose):
    """
    load records from log file.
    """
    config_object = load_config(config)
    config_file_path = config_object['system_time_line']['smslog_local_collector']['config']
    if config_file_path.startswith('.'):
        config_file_path = pathlib.Path(pathlib.Path(config).parent, config_file_path)
    collector_config = load_collector_config(str(config_file_path))
    collect_log_from_local_file_by_range(collector_config, owner, repo, log_file, begin_date, end_date, verbose)


@cli.command()
@click.option('-c', '--config', required=True, help='config file path')
@click.option('-o', '--owner', help='owner name')
@click.option('-r', '--repo', help='repo name')
@click.option('-l', '--log-file', required=True, help='log file path')
def log_file_info(config, owner, repo, log_file):
    """
    Get info from log file.
    """
    config_object = load_config(config)
    config_file_path = config_object['system_time_line']['smslog_local_collector']['config']
    if config_file_path.startswith('.'):
        config_file_path = pathlib.Path(pathlib.Path(config).parent, config_file_path)
    collector_config = load_collector_config(str(config_file_path))
    log_info = get_log_info_from_local_file(collector_config, owner, repo, log_file, "json")
    result = {
        'app': 'sms_local_collector',
        'timestamp': datetime.datetime.now().timestamp(),
        'data': {
            'request': {
                'command': 'log_file_info',
                'config': config,
                'owner': owner,
                'repo': repo,
                'log_file': log_file
            },
            'response': log_info
        }
    }
    print(json.dumps(result, indent=2))


@cli.command()
@click.option('-c', '--config', help='config file path')
@click.option('-o', '--owner', help='owner name')
@click.option('-r', '--repo', help='repo name')
@click.option('--begin-date', help='begin date, [start_date, end_date), YYYY-MM-dd')
@click.option('--end-date', help='end date, [start_date, end_date), YYYY-MM-dd')
def process(config, owner, repo, begin_date, end_date):
    """
    process log records.
    """
    config_object = load_config(config)
    config_file_path = config_object['system_time_line']['processor']['run_time_line']['config']
    if config_file_path.startswith('.'):
        config_file_path = pathlib.Path(pathlib.Path(config).parent, config_file_path)
    processor_config = load_processor_config(str(config_file_path))

    begin_date = datetime.datetime.strptime(begin_date, "%Y-%m-%d")
    end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d")
    days_count = (end_date-begin_date).days
    date_list = [begin_date + datetime.timedelta(days=x) for x in range(0, days_count)]
    for query_date in date_list:
        print(query_date.strftime("%Y-%m-%d"))
        time_line_processor(processor_config, owner, repo, query_date, None, True, False)


@cli.command()
@click.option('-c', '--config', help='config file path')
@click.option('--begin-date', help='begin date, [start_date, end_date), YYYY-MM-dd')
@click.option('--end-date', help='end date, [start_date, end_date), YYYY-MM-dd')
@click.option("--output-dir", required=True, help="output directory path")
@click.option("--save-to-db", is_flag=True, default=False, help="save result to database")
@click.option("-p", "--print", "print_flag", is_flag=True, default=False, help="print result to console.")
def generate(config, begin_date, end_date, output_dir, save_to_db, print_flag):
    """
    Generate chart data.
    """
    config_object = load_config(config)
    config_file_path = config_object['system_time_line']['processor']['run_time_line']['config']
    if config_file_path.startswith('.'):
        config_file_path = pathlib.Path(pathlib.Path(config).parent, config_file_path)
    processor_config = load_processor_config(str(config_file_path))

    begin_date = datetime.datetime.strptime(begin_date, "%Y-%m-%d")
    end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d")
    days_count = (end_date-begin_date).days
    date_list = [begin_date + datetime.timedelta(days=x) for x in range(0, days_count)]
    for query_date in date_list:
        print(query_date.strftime("%Y-%m-%d"))
        output_file = pathlib.Path(output_dir, query_date.strftime("%Y-%m-%d") + '.json')
        generate_chart_data(processor_config, query_date, output_file, save_to_db, print_flag)


@cli.command()
@click.option('-c', '--config', help='config file path')
@click.option('--begin-date', help='begin date, [start_date, end_date), YYYY-MM-dd')
@click.option('--end-date', help='end date, [start_date, end_date), YYYY-MM-dd')
@click.option('--data-dir', help='date dir')
@click.option("--output-dir", required=True, help="output directory path")
def plot(config, begin_date, end_date, data_dir, output_dir):
    """
    plot chart in svg and png.
    """
    config_object = load_config(config)
    plot_script = config_object['system_time_line']['plotter']['script']
    if plot_script.startswith('.'):
        plot_script = pathlib.Path(pathlib.Path(config).parent, plot_script)

    begin_date = datetime.datetime.strptime(begin_date, "%Y-%m-%d")
    end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d")
    days_count = (end_date-begin_date).days
    date_list = [begin_date + datetime.timedelta(days=x) for x in range(0, days_count)]
    for query_date in date_list:
        print(query_date.strftime("%Y-%m-%d"))
        data_file = "{date}.json".format(date=query_date.strftime("%Y-%m-%d"))
        data_file = pathlib.Path(data_dir, data_file)
        output_svg_file = "{date}.svg".format(date=query_date.strftime("%Y-%m-%d"))
        output_svg_file = pathlib.Path(output_dir, output_svg_file)
        output_png_file = "{date}.png".format(date=query_date.strftime("%Y-%m-%d"))
        output_png_file = pathlib.Path(output_dir, output_png_file)

        subprocess.call([
            'node',
            str(plot_script),
            '--data=' + str(data_file),
            '--output-svg=' + str(output_svg_file),
            '--output-png=' + str(output_png_file)
        ])


if __name__ == "__main__":
    cli()
