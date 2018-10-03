#!/usr/bin/env python
# coding=utf-8
"""
NWPC LOG 分析工具

调用 Spark 应用分析日志条目
    spark_node_status：生成节点状态
"""
from __future__ import absolute_import
from fabric.api import run, cd, execute, env, settings

import datetime
import argparse
import sys


def get_spark_node_status(owner, repo, start_date, end_date):
    """
    统计节点运行状态，使用 spark 脚本 spark_node_status.py。
    :param owner:
    :param repo:
    :param start_date:
    :param end_date:
    :return:
    """
    # 常量参数
    spark_home = '/home/vagrant/app/spark/spark-1.3.0-bin-hadoop2.4'
    spark_processor_dir = "/vagrant/nwpc_log_processor/spark_processor"
    base_dir = "/vagrant"
    py_files_name = "nwpc_work_flow_model.zip"
    spark_script_file_name = "spark_node_status.py"

    if end_date is None:
        end_date = start_date

    env.hosts = ['vagrant@localhost']
    env.password = 'vagrant'

    def run_spark_node_status(owner, repo, start_date, end_date):
        py_files = "{base_dir}/{py_files_name}".format(base_dir=base_dir, py_files_name=py_files_name)

        with cd(base_dir):
            run('zip -r {py_files_name} nwpc_work_flow_model'.format(py_files_name=py_files_name))

        with cd(spark_processor_dir):
            run('{spark_home}/bin/spark-submit --master local[4] --py-files {py_files} {spark_script_file_name} '
                '-u {owner} -r {repo} --start-date={start_date} --end-date={end_date}'.format(
                spark_home=spark_home,
                py_files=py_files,
                spark_script_file_name=spark_script_file_name,
                owner=owner,
                repo=repo,
                start_date=start_date,
                end_date=end_date
            ))

    execute(run_spark_node_status, owner=owner, repo=repo, start_date=start_date, end_date=end_date)



def nwpc_log_analysis_tool():
    """
    NWPC 日志分析命令行工具主函数
    :return:
    """
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description="""\
DESCRIPTION
    A analysis toll for nwpc log project.""")

    repo_user_parser = argparse.ArgumentParser(add_help=False)
    repo_user_parser.add_argument("-u", "--user", help="user name", required=True)
    repo_user_parser.add_argument("-r", "--repo", help="repo name", required=True)

    subparsers = parser.add_subparsers(title='NWPC log analysis commands',  dest='sub_command')

    # node_status
    parser_node_status = subparsers.add_parser('node_status',
                                          parents=[repo_user_parser],
                                          description="Calculate node status from start_date to end_date in Hive.")
    parser_node_status.add_argument("--start-date", help="start date, YYYY-MM-DD", required=True)
    parser_node_status.add_argument("--end-date", help="end date, YYYY-MM-DD", required=True)


    args = parser.parse_args()

    user_name = None
    repo_name = None
    start_date = None
    end_date = None

    if args.sub_command=='node_status':
        if args.user:
            user_name = args.user
        if args.repo:
            repo_name = args.repo
        if args.start_date:
            start_date = args.start_date
        if args.end_date:
            end_date = args.end_date
        get_spark_node_status(user_name, repo_name, start_date, end_date)



if __name__ == "__main__":
    nwpc_log_analysis_tool()