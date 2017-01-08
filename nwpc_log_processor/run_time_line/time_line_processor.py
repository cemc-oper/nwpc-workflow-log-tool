# coding=utf-8
import argparse
import datetime
import json
import os

from pymongo import MongoClient
from sqlalchemy import asc, or_

# settings
from nwpc_log_processor.run_time_line.conf.config import session, MONGODB_HOST, MONGODB_PORT

# mysql tables
from nwpc_log_model.rdbms_model import Record
from nwpc_log_model.util.bunch_util import BunchUtil

# mongodb collections
mongodb_client = MongoClient(MONGODB_HOST, MONGODB_PORT)
smslog_mongodb = mongodb_client.smslog

daily_tree_status_collection = smslog_mongodb.daily_tree_status_collection
daily_repo_time_line_collection = smslog_mongodb.daily_repo_time_line_collection
repo_time_line_collection = smslog_mongodb.daily_repo_time_line_collection


def get_bunch(owner_name, repo_name, query_date):
    """
    从 MySQL 中生成某个项目的树形结构，保存到 MongoDB 中。

    :param owner_name:
    :param repo_name:
    :param query_date:
    :return: Bunch
    """

    # generate tree
    tree = BunchUtil.generate_repo_tree_from_session(owner_name, repo_name, query_date, session)

    # add daily tree structure to mongodb
    tree_status_key = {
        'owner': owner_name,
        'repo': repo_name,
        'date': query_date
    }
    tree_status = {
        'owner': owner_name,
        'repo': repo_name,
        'date': query_date,
        'tree': tree.to_dict()
    }
    daily_tree_status_collection.update(tree_status_key, tree_status, upsert=True)
    return tree


def load_schema(owner, repo):
    config_file_name = "{owner}.{repo}.schema.json".format(
        owner=owner, repo=repo
    )
    config_file_dir = os.path.join(os.path.dirname(__file__), 'conf')

    config_file_path = os.path.join(config_file_dir, config_file_name)
    if not os.path.exists(config_file_path):
        print("{config_file_path} doesn't exist".format(config_file_path=config_file_path))
        return None
    with open(config_file_path, 'r') as config_file:
        schema = json.load(config_file)
        return schema


def calculate_time_line(owner, repo, query_date, schema, bunch):
    """
    sms服务器
    :param owner:
    :param repo:
    :param query_date:
    :param schema:
    :param bunch:
    :return:
    """
    suites = schema["suites"]

    result = {
        "owner": owner,
        "repo": repo,
        "query_date": query_date.strftime("%Y-%m-%d"),
        "suites": []
    }

    for a_suite in suites:
        suite_time_line = calculate_suite_time_line(owner, repo, query_date, a_suite, bunch)
        result["suites"].append(suite_time_line)

    return result


def calculate_suite_time_line(owner, repo, query_date, suite_schema, bunch):
    """
    每个suite
    :param owner:
    :param repo:
    :param query_date:
    :param suite_schema:
    :param bunch:
    :return:
    """
    result = {
        'name': suite_schema['name'],
        'label': suite_schema['name'],
        'times': []
    }
    print("{owner}/{repo}/{suite}".format(
        owner=owner,
        repo=repo,
        suite=suite_schema['name']
    ))

    for a_run_time in suite_schema["times"]:
        run_time_result = calculate_run_time_line(owner, repo, query_date, a_run_time, bunch)
        if run_time_result is not None:
            result["times"].extend(run_time_result)

    return result


def calculate_run_time_line(owner, repo, query_date, run_time_line_schema, bunch):
    """
    每个 run_time 项目，用在两个地方：
        每个时次
        每个时次中的并行作业

    :param owner:
    :param repo:
    :param query_date:
    :param run_time_line_schema:
    :param bunch:
    :return:
    """
    result = {
        'name': run_time_line_schema['name'],
        'label': run_time_line_schema['label'],
        'start_time': None,
        'end_time': None,
        'class': run_time_line_schema['class'],
    }

    start_time = calculate_run_time_point(owner, repo, query_date, run_time_line_schema['start_time'], bunch)
    end_time = calculate_run_time_point(owner, repo, query_date, run_time_line_schema['end_time'], bunch)

    if start_time is None or end_time is None:
        print("[calculate_run_time_line] some time is None:")
        print("\trun_time_line_schema:", run_time_line_schema)
        return None

    if len(start_time) == 1 and len(end_time) == 1:
        result["start_time"] = start_time[0]
        result["end_time"] = end_time[0]

        if 'times' in run_time_line_schema:
            run_times_result = []
            for a_run_time in run_time_line_schema['times']:
                run_times_result = calculate_run_time_line(owner, repo, query_date, a_run_time, bunch)
            result["times"] = run_times_result

        return [result]

    elif len(start_time) == 2 and len(end_time) == 2:
        result_pre_day = {
            'name': run_time_line_schema['name'],
            'label': run_time_line_schema['label'],
            'start_time': start_time[0],
            'end_time': end_time[0],
            'class': run_time_line_schema['class'],
        }

        result["start_time"] = start_time[1]
        result["end_time"] = end_time[1]

        if 'times' in run_time_line_schema:
            run_times_result = []
            run_times_result_pre_day = []
            for a_run_time in run_time_line_schema['times']:
                children_run_time_line = calculate_run_time_line(owner, repo, query_date, a_run_time, bunch)

                if len(children_run_time_line) == 1:
                    child_run_time_line = children_run_time_line[0]
                    if result["start_time"] <= child_run_time_line["start_time"] <= result["end_time"]:
                        run_times_result.append(child_run_time_line)
                    elif result_pre_day["start_time"] <= child_run_time_line["start_time"] <= result_pre_day["end_time"]:
                        run_times_result_pre_day.append(child_run_time_line)
                    else:
                        print("Fatal Error: children_run_time_line error")
                        print("\tchild_run_time_line:", child_run_time_line)
                        print("\tresult:", result)
                        return None

                elif len(children_run_time_line) == 2:
                    run_times_result.append(children_run_time_line[1])
                    run_times_result_pre_day.append(children_run_time_line[0])

            result["times"] = run_times_result
            result_pre_day["times"] = run_times_result_pre_day

        return [result_pre_day, result]
    elif len(start_time) == 1 and len(end_time) == 2:
        start_time.insert(0, "00:00:00")
        result_pre_day = {
            'name': run_time_line_schema['name'],
            'label': run_time_line_schema['label'],
            'start_time': start_time[0],
            'end_time': end_time[0],
            'class': run_time_line_schema['class'],
        }

        result["start_time"] = start_time[1]
        result["end_time"] = end_time[1]

        if 'times' in run_time_line_schema:
            run_times_result = []
            run_times_result_pre_day = []
            for a_run_time in run_time_line_schema['times']:
                children_run_time_line = calculate_run_time_line(owner, repo, query_date, a_run_time, bunch)

                if len(children_run_time_line) == 1:
                    child_run_time_line = children_run_time_line[0]
                    if result["start_time"] <= child_run_time_line["start_time"] <= result["end_time"]:
                        run_times_result.append(child_run_time_line)
                    elif result_pre_day["start_time"] <= child_run_time_line["start_time"] <= result_pre_day["end_time"]:
                        run_times_result_pre_day.append(child_run_time_line)
                    else:
                        print("Fatal Error: children_run_time_line error")
                        print("\tchild_run_time_line:", child_run_time_line)
                        print("\tresult:", result)
                        return None

                elif len(children_run_time_line) == 2:
                    run_times_result.append(children_run_time_line[1])
                    run_times_result_pre_day.append(children_run_time_line[0])

            result["times"] = run_times_result
            result_pre_day["times"] = run_times_result_pre_day

        return [result_pre_day, result]
    else:
        print('Fatal Error: start time and end time not fit')
        print('\trun_time_line_schema:',run_time_line_schema)
        return None


def calculate_run_time_point(owner, repo, query_date, run_time_schema, bunch):
    """
    计算单个时间，用于以下两项：
        start_time
        end_time

    :param owner:
    :param repo:
    :param query_date:
    :param run_time_schema:
        {
            "operator": "start",
            "command": "submitted",
            "paths": [
                {
                    "operator": "equal", # or like
                    "node_type": "family" # or node
                    "node_path": "/gda_gsi_v1r5/T639/00"
                }
            ]
        }
    :param bunch:
    :return:
    """
    Record.prepare(owner, repo)
    operator = run_time_schema["operator"]
    command = run_time_schema["command"]
    paths = run_time_schema["paths"]

    query = session.query(Record)

    if len(paths) == 1:
        node_path_object = paths[0]
        if node_path_object['operator'] == 'equal':
            query = query.filter(Record.record_fullname.like(node_path_object['node_path']))
        elif node_path_object['operator'] == 'like':
            query = query.filter(Record.record_fullname.like(node_path_object['node_path']))
        else:
            print("Fatal Error: paths operator is unknown")
            print("\tnode path:", node_path_object)
            return None

    elif len(paths) > 1:
        path_filters = []
        for node_path_object in paths:
            if node_path_object['operator'] == 'equal':
                path_filters.append(Record.record_fullname.like(node_path_object['node_path']))
            elif node_path_object['operator'] == 'like':
                path_filters.append(Record.record_fullname.like(node_path_object['node_path']))
            else:
                print("Fatal Error: paths operator is unknown")
                print("\tnode path:", node_path_object)
                return None
        query = query.filter(or_(*path_filters))

    else:
        print("Fatal Error: paths is empty")
        print("\trun time schema:", run_time_schema)
        return None

    query = query.filter(Record.record_date == query_date.date()) \
        .filter(Record.record_command.in_(['submitted', 'complete'])) \
        .order_by(asc(Record.record_time)) \
        .order_by(asc(Record.line_no))

    records = query.all()
    record_length = len(records)

    if record_length == 0:
        print("There is no record")
        print("\trun time schema:", run_time_schema)
        return None

    if records[-1].record_command == 'complete':
        # 任务在当天内结束, submitted -> complete

        if operator == "end":
            return [records[-1].record_time.strftime("%H:%M:%S")]

        elif operator == 'start':
            current_index = 0
            while current_index < record_length:
                if records[current_index].record_command == 'complete':
                    break
                current_index += 1

            if current_index != record_length - 1:
                # 之前有complete
                if records[current_index].record_fullname == records[-1].record_fullname:
                    # 单个node情况，之前有complete，寻找之后的submitted
                    pass
                else:
                    # 多个node情况，第一个submitted为start时间
                    current_index = 0

                while current_index < record_length:
                    if records[current_index].record_command == 'submitted':
                        break
                    current_index += 1

                if current_index == record_length:
                    return [records[-1].record_time.strftime("%H:%M:%S")]
                else:
                    return [records[current_index].record_time.strftime("%H:%M:%S")]

            else:
                # 之前没有complete，正常情况，第一个submitted为start时间
                current_index = 0

                while current_index < record_length:
                    if records[current_index].record_command == 'submitted':
                        break
                    current_index += 1
                if current_index == record_length:
                    return [records[-1].record_time.strftime("%H:%M:%S")]
                else:
                    return [records[current_index].record_time.strftime("%H:%M:%S")]

    else:
        # 任务时间跨过零点，需要分裂为两个项目
        current_index = 0
        while current_index < record_length:
            if records[current_index].record_command == 'complete':
                break
            current_index += 1

        if current_index == record_length:
            print("Fatal Error: command parser failed to find complete")
            print("\trun_time_schema:", run_time_schema)
            return None

        if operator == "end":
            return [records[current_index].record_time.strftime("%H:%M:%S"), "23:59:00"]

        elif operator == "start":
            # 第一个complete

            # 第一个submitted
            while current_index < record_length:
                if records[current_index].record_command == 'submitted':
                    break
                current_index += 1
            if current_index == record_length:
                print("Fatal Error: command parser failed to find submitted")
                print("\trun_time_schema:", run_time_schema)
                return None
            return ["00:00:00", records[current_index].record_time.strftime("%H:%M:%S")]

    return None


def process_time_line(owner_name, repo_name, query_date):
    bunch = get_bunch(owner_name, repo_name, query_date)

    schema = load_schema(owner_name, repo_name)
    if schema is None:
        print("Fatal Error: load schema failed.")
        return None

    result = calculate_time_line(owner_name, repo_name, query_date, schema, bunch)
    return result


def save_time_line_to_db(time_line_result):
    key = {
        'owner': time_line_result['owner'],
        'repo': time_line_result['repo'],
        'query_date': time_line_result['query_date']
    }
    value = time_line_result
    daily_repo_time_line_collection.replace_one(key, value, upsert=True)


def save_time_line_to_file(time_line_result, output_file_path):
    output_file_content = json.dumps(time_line_result, indent=2)
    with open(output_file_path, 'w') as output_file:
        output_file.write(output_file_content)


def main():
    start_time = datetime.datetime.now()

    # argument parser
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description="""\
DESCRIPTION
    Calculate time line for owner/repo on query date according to config file.""")

    parser.add_argument("-o", "--owner", help="owner name", required=True)
    parser.add_argument("-r", "--repo", help="repo name", required=True)
    parser.add_argument("-d", "--date", help="query date (%Y-%m-%d)", required=True)
    parser.add_argument("--output-file", help="output file path")
    parser.add_argument("--save-to-db", help="save result to database", action='store_true')
    parser.add_argument("-p", "--print", dest='print_flag', help="print result to console.", action='store_true')

    # parser section

    args = parser.parse_args()

    owner_name = args.owner
    print('owner name: {owner_name}'.format(owner_name=owner_name))

    repo_name = args.repo
    print('repo name: {repo_name}'.format(repo_name=repo_name))

    query_date = datetime.datetime.strptime(args.date, "%Y-%m-%d")
    print('query date: {query_date}'.format(query_date=query_date))

    # calculate section

    result = process_time_line(owner_name, repo_name, query_date)
    if result is None:
        return

    # output section

    if args.print_flag:
        print(json.dumps(result, indent=4))

    # 保存到 MongoDB
    if args.save_to_db:
        print("Save results to database")
        save_time_line_to_db(result)

    # 保存到文件
    if args.output_file:
        print("Write results to output file: {output_file_path}".format(output_file_path=args.output_file))
        save_time_line_to_file(result, args.output_file)

    # 时间统计
    end_time = datetime.datetime.now()
    print(end_time - start_time)
    return


if __name__ == "__main__":
    main()
