# coding: utf-8
import click
from sqlalchemy import or_, asc


def calculate_time_line(owner, repo, record_class, query_date, schema, bunch, session):
    """
    sms服务器
    :param owner:
    :param repo:
    :param record_class:
    :param query_date:
    :param schema:
    :param bunch:
    :param session:
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
        suite_time_line = calculate_suite_time_line(owner, repo, record_class, query_date, a_suite, bunch, session)
        result["suites"].append(suite_time_line)

    return result


def calculate_suite_time_line(owner, repo, record_class, query_date, suite_schema, bunch, session):
    """
    每个suite
    :param owner:
    :param repo:
    :param record_class:
    :param query_date:
    :param suite_schema:
    :param bunch:
    :param session:
    :return:
    """
    result = {
        'name': suite_schema['name'],
        'label': suite_schema['name'],
        'times': []
    }
    click.echo("{owner}/{repo}/{suite}".format(
        owner=owner,
        repo=repo,
        suite=suite_schema['name']
    ))

    for a_run_time in suite_schema["times"]:
        run_time_result = calculate_run_time_line(owner, repo, record_class, query_date, a_run_time, bunch, session)
        if run_time_result is not None:
            result["times"].extend(run_time_result)

    return result


def calculate_run_time_line(owner, repo, record_class, query_date, run_time_line_schema, bunch, session):
    """
    每个 run_time 项目，用在两个地方：
        每个时次
        每个时次中的并行作业

    :param owner:
    :param repo:
    :param record_class:
    :param query_date:
    :param run_time_line_schema:
    :param bunch:
    :param session:
    :return:
    """
    result = {
        'name': run_time_line_schema['name'],
        'label': run_time_line_schema['label'],
        'start_time': None,
        'end_time': None,
        'class': run_time_line_schema['class'],
    }

    start_time = calculate_run_time_point(owner, repo, record_class, query_date, run_time_line_schema['start_time'], bunch, session)
    end_time = calculate_run_time_point(owner, repo, record_class, query_date, run_time_line_schema['end_time'], bunch, session)

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
                run_times_result = calculate_run_time_line(owner, repo, record_class, query_date, a_run_time, bunch, session)
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
                children_run_time_line = calculate_run_time_line(owner, repo, record_class, query_date, a_run_time, bunch, session)

                if len(children_run_time_line) == 1:
                    child_run_time_line = children_run_time_line[0]
                    if result["start_time"] <= child_run_time_line["start_time"] <= result["end_time"]:
                        run_times_result.append(child_run_time_line)
                    elif result_pre_day["start_time"] <= child_run_time_line["start_time"] \
                            <= result_pre_day["end_time"]:
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
                children_run_time_line = calculate_run_time_line(owner, repo, record_class, query_date, a_run_time, bunch, session)

                if len(children_run_time_line) == 1:
                    child_run_time_line = children_run_time_line[0]
                    if result["start_time"] <= child_run_time_line["start_time"] <= result["end_time"]:
                        run_times_result.append(child_run_time_line)
                    elif result_pre_day["start_time"] <= child_run_time_line["start_time"] \
                            <= result_pre_day["end_time"]:
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
        print('\trun_time_line_schema:', run_time_line_schema)
        return None


def calculate_run_time_point(owner, repo, record_class, query_date, run_time_schema, bunch, session):
    """
    计算单个时间，用于以下两项：
        start_time
        end_time

    :param owner:
    :param repo:
    :param record_class:
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
    :param session:
    :return:
    """
    record_class.prepare(owner, repo)
    operator = run_time_schema["operator"]
    command = run_time_schema["command"]
    paths = run_time_schema["paths"]

    query = session.query(record_class)

    if len(paths) == 1:
        node_path_object = paths[0]
        if node_path_object['operator'] == 'equal':
            query = query.filter(record_class.node_path.like(node_path_object['node_path']))
        elif node_path_object['operator'] == 'like':
            query = query.filter(record_class.node_path.like(node_path_object['node_path']))
        else:
            print("Fatal Error: paths operator is unknown")
            print("\tnode path:", node_path_object)
            return None

    elif len(paths) > 1:
        path_filters = []
        for node_path_object in paths:
            if node_path_object['operator'] == 'equal':
                path_filters.append(record_class.node_path.like(node_path_object['node_path']))
            elif node_path_object['operator'] == 'like':
                path_filters.append(record_class.node_path.like(node_path_object['node_path']))
            else:
                print("Fatal Error: paths operator is unknown")
                print("\tnode path:", node_path_object)
                return None
        query = query.filter(or_(*path_filters))

    else:
        print("Fatal Error: paths is empty")
        print("\trun time schema:", run_time_schema)
        return None

    query = query.filter(record_class.date == query_date.date()) \
        .filter(record_class.command.in_(['submitted', 'complete'])) \
        .order_by(asc(record_class.time)) \
        .order_by(asc(record_class.line_no))

    records = query.all()
    record_length = len(records)

    if record_length == 0:
        print("There is no record")
        print("\trun time schema:", run_time_schema)
        return None

    if records[-1].command == 'complete':
        # 任务在当天内结束, submitted -> complete

        if operator == "end":
            return [records[-1].time.strftime("%H:%M:%S")]

        elif operator == 'start':
            current_index = 0
            while current_index < record_length:
                if records[current_index].command == 'complete':
                    break
                current_index += 1

            if current_index != record_length - 1:
                # 之前有complete
                if records[current_index].node_path == records[-1].node_path:
                    # 单个node情况，之前有complete，寻找之后的submitted
                    pass
                else:
                    # 多个node情况，第一个submitted为start时间
                    current_index = 0

                while current_index < record_length:
                    if records[current_index].command == 'submitted':
                        break
                    current_index += 1

                if current_index == record_length:
                    return [records[-1].time.strftime("%H:%M:%S")]
                else:
                    return [records[current_index].time.strftime("%H:%M:%S")]

            else:
                # 之前没有complete，正常情况，第一个submitted为start时间
                current_index = 0

                while current_index < record_length:
                    if records[current_index].command == 'submitted':
                        break
                    current_index += 1
                if current_index == record_length:
                    return [records[-1].time.strftime("%H:%M:%S")]
                else:
                    return [records[current_index].time.strftime("%H:%M:%S")]

    else:
        # 任务时间跨过零点，需要分裂为两个项目
        current_index = 0
        while current_index < record_length:
            if records[current_index].command == 'complete':
                break
            current_index += 1

        if current_index == record_length:
            print("Fatal Error: command parser failed to find complete")
            print("\trun_time_schema:", run_time_schema)
            return None

        if operator == "end":
            return [records[current_index].time.strftime("%H:%M:%S"), "23:59:00"]

        elif operator == "start":
            # 第一个complete

            # 第一个submitted
            while current_index < record_length:
                if records[current_index].command == 'submitted':
                    break
                current_index += 1
            if current_index == record_length:
                print("Fatal Error: command parser failed to find submitted")
                print("\trun_time_schema:", run_time_schema)
                return None
            return ["00:00:00", records[current_index].time.strftime("%H:%M:%S")]

    return None
