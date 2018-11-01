# coding=utf-8
import datetime

from sqlalchemy import asc

from nwpc_workflow_log_model.rmdb.sms.record import SmsRecord


class NodeSituationUtil(object):
    def __init__(self):
        pass

    @staticmethod
    def get_task_node_situation_from_record_list(node_situation, node_path, query_date_object, record_list):
        """
        :param node_situation:
            {
                'owner': self.owner,
                'repo': self.repo,
                'node': node_path,
                'date': self.query_date,
                'type': 'unknown',
                'time_point': {},
                'time_period': {}
            }
        :param node_path:
        :param query_date_object:
        :param record_list: 多个日期的日志条目集合，需要确保每个日志的条目都按时间排序，本方法假定已经排好序，而不检测日志是否排序。
        :return:
        """
        # test
        # print "TASK_NODE_STATUS:", query_date_object
        # for i in record_list:
        #     print i.date, i.log_record
        records = list(filter(lambda record: record.date == query_date_object, record_list))
        # print "TASK_NODE_STATUS: after filter"
        # print records

        # find error in records.
        error_found = False
        error_command = ['aborted']
        for a_record in records:
            if a_record.command in error_command:
                error_found = True
                break
        if error_found:
            node_situation['situation'] = 'error'
            return node_situation

        i = 0
        # find submitted
        submitted_index = -1
        while i < len(records):
            if records[i].command == 'submitted':
                submitted_index = i
                break
            i += 1
        if submitted_index == -1:
            # don't find submitted
            node_situation['situation'] = 'unknown'

            # CASE: complete by rule
            # line_no   record_string
            # 3350020   # LOG:[09:15:37 1.2.2015] complete:/gmf_grapes_v1423/grapes_global/00/model/si_incr
            # 3350021   # MSG:[09:15:37 1.2.2015] complete:/gmf_grapes_v1423/grapes_global/00/model/si_incr by rule

            # find complete
            complete_index = -1
            cur_index = 0
            while cur_index < len(records):
                if records[cur_index].command == 'complete':
                    complete_index = cur_index
                    break
                cur_index += 1
            if complete_index != -1 and (complete_index+1) < len(records) \
                    and records[complete_index+1].command == "complete" \
                    and records[complete_index+1].additional_information == "by rule":
                node_situation['situation'] = 'complete by rule'
            return node_situation

        # find active
        active_index = -1
        i += 1
        while i < len(records):
            if records[i].command == 'active':
                active_index = i
                break
            i += 1
        if active_index == -1:
            # don't find active
            node_situation['situation'] = 'unknown'
            return node_situation

        # find complete
        complete_index = -1
        i += 1
        while i < len(records):
            if records[i].command == 'complete':
                complete_index = i
                break
            i += 1
        if complete_index == -1:
            # don't find active
            node_situation['situation'] = 'unknown'
            return node_situation

        # the simplest situation.
        node_situation['situation'] = 'normal'

        submitted_time = datetime.datetime.combine(records[submitted_index].date,
                                                   records[submitted_index].time)
        active_time = datetime.datetime.combine(records[active_index].date,
                                                records[active_index].time)
        complete_time = datetime.datetime.combine(records[complete_index].date,
                                                  records[complete_index].time)

        if 'time_point' not in node_situation:
            node_situation['time_point'] = {}

        node_situation['time_point']['submitted'] = submitted_time
        node_situation['time_point']['active'] = active_time
        node_situation['time_point']['complete'] = complete_time

        time_in_submitted = active_time - submitted_time
        time_in_active = complete_time - active_time
        time_in_all = complete_time - submitted_time

        if 'time_period' not in node_situation:
            node_situation['time_period'] = {}

        node_situation['time_period']['in_submitted'] = {
            'days': time_in_submitted.days,
            'seconds': time_in_submitted.seconds}
        node_situation['time_period']['in_active'] = {
            'days': time_in_active.days,
            'seconds': time_in_active.seconds}
        node_situation['time_period']['in_all'] = {
            'days': time_in_all.days,
            'seconds': time_in_all.seconds}

        return node_situation

    @staticmethod
    def get_task_node_situation_from_session(node_situation, node_path, date_object, session):
        """
        :param node_situation:
            {
                'owner': self.owner,
                'repo': self.repo,
                'node': node_path,
                'date': self.query_date,
                'type': 'unknown',
                'time_point': {},
                'time_period': {}
            }
        :param node_path:
        :param date_object:
        :param session:
        :return:
        """
        SmsRecord.prepare(node_situation['owner'], node_situation['repo'])
        query = session.query(SmsRecord) \
            .filter(SmsRecord.node_path == node_path) \
            .filter(SmsRecord.date == date_object) \
            .filter(SmsRecord.command.in_(['submitted', 'active', 'complete', 'aborted'])) \
            .order_by(asc(SmsRecord.time)) \
            .order_by(asc(SmsRecord.line_no))
        records = query.all()

        return NodeSituationUtil.get_task_node_situation_from_record_list(node_situation, node_path, date_object, records)

    @staticmethod
    def get_family_node_situation_from_session(node_situation, node_path, date_object, session):
        query = session.query(SmsRecord) \
            .filter(SmsRecord.node_path == node_path) \
            .filter(SmsRecord.date == date_object) \
            .filter(SmsRecord.command.in_(['submitted', 'active', 'complete', 'aborted'])) \
            .order_by(asc(SmsRecord.time))
        records = query.all()

        if len(records) == 0:
            return node_situation

        # find error in records.
        error_found = False
        error_command = ['aborted']
        for a_record in records:
            if a_record.command in error_command:
                error_found = True
                break
        if error_found:
            node_situation['situation'] = 'error'
            return node_situation

        # normal family which don't cross 0:00
        if records[-1].command == "complete":
            if records[0].command == "submitted":
                submitted_index = 0
                submitted_time = datetime.datetime.combine(records[submitted_index].date,
                                                           records[submitted_index].time)
                complete_index = -1
                complete_time = datetime.datetime.combine(records[complete_index].date,
                                                          records[complete_index].time)

                if 'time_point' not in node_situation:
                    node_situation['time_point'] = {}

                node_situation['time_point']['submitted'] = submitted_time
                node_situation['time_point']['complete'] = complete_time

                time_in_all = complete_time - submitted_time

                if 'time_period' not in node_situation:
                    node_situation['time_period'] = {}

                node_situation['time_period']['in_all'] = {'days': time_in_all.days,
                                                        'seconds': time_in_all.seconds}
                node_situation['situation'] = 'normal'

        # normal family which cross 0:00
        else:
            # find complete
            command_to_found = ['complete']
            last_day_complete_command_index = -1
            for i in range(0, len(records)):
                a_record = records[i]
                if a_record.command in command_to_found:
                    last_day_complete_command_index = i
                    break
            if last_day_complete_command_index == -1:
                # unknown
                return node_situation
            submitted_index = last_day_complete_command_index + 1
            if records[submitted_index].command != 'submitted':
                # unknown
                return node_situation
            submitted_time = datetime.datetime.combine(records[submitted_index].date,
                                                       records[submitted_index].time)

            # find this day complete
            next_date = date_object + datetime.timedelta(days=1)
            next_date_query = session.query(SmsRecord) \
                .filter(SmsRecord.node_path == node_path) \
                .filter(SmsRecord.date == next_date) \
                .filter(SmsRecord.command.in_(['submitted', 'active', 'complete', 'aborted'])) \
                .order_by(asc(SmsRecord.time))
            next_date_records = next_date_query.all()

            # find complete
            command_to_found = ['complete', 'aborted']
            this_day_command_index = -1
            for i in range(0, len(next_date_records)):
                a_record = next_date_records[i]
                if a_record.command in command_to_found:
                    this_day_command_index = i
                    break
            if this_day_command_index == -1:
                # unknown
                return node_situation

            # error
            if next_date_records[this_day_command_index].command == "aborted":
                node_situation['situation'] = 'error'
                return node_situation

            # normal
            complete_index = this_day_command_index
            complete_time = datetime.datetime.combine(next_date_records[complete_index].date,
                                                      next_date_records[complete_index].time)

            if 'time_point' not in node_situation:
                    node_situation['time_point'] = {}
            node_situation['time_point']['submitted'] = submitted_time
            node_situation['time_point']['complete'] = complete_time

            time_in_all = complete_time - submitted_time
            if 'time_period' not in node_situation:
                    node_situation['time_period'] = {}
            node_situation['time_period']['in_all'] = {'days': time_in_all.days,
                                                    'seconds': time_in_all.seconds}
            node_situation['situation'] = 'normal'

        return node_situation

    @staticmethod
    def get_family_node_situation_from_record_list(node_situation, node_path, query_date_object, record_list):
        cur_date_records = list(filter(lambda record: record.date == query_date_object, record_list))

        next_date = query_date_object + datetime.timedelta(days=1)
        next_date_records = list(filter(lambda record: record.date == next_date, record_list))

        if len(cur_date_records) == 0:
            return node_situation

        # find error in records.
        error_found = False
        error_command = ['aborted']
        for a_record in cur_date_records:
            if a_record.command in error_command:
                error_found = True
                break
        if error_found:
            node_situation['situation'] = 'error'
            return node_situation

        # normal family which don't cross 0:00
        if cur_date_records[-1].command == "complete":
            if cur_date_records[0].command == "submitted":
                submitted_index = 0
                submitted_time = datetime.datetime.combine(cur_date_records[submitted_index].date,
                                                           cur_date_records[submitted_index].time)
                complete_index = -1
                complete_time = datetime.datetime.combine(cur_date_records[complete_index].date,
                                                          cur_date_records[complete_index].time)

                if 'time_point' not in node_situation:
                    node_situation['time_point'] = {}

                node_situation['time_point']['submitted'] = submitted_time
                node_situation['time_point']['complete'] = complete_time

                time_in_all = complete_time - submitted_time

                if 'time_period' not in node_situation:
                    node_situation['time_period'] = {}

                node_situation['time_period']['in_all'] = {'days': time_in_all.days,
                                                        'seconds': time_in_all.seconds}
                node_situation['situation'] = 'normal'

        # normal family which cross 0:00
        else:
            # find complete
            command_to_found = ['complete']
            last_day_complete_command_index = -1
            for i in range(0, len(cur_date_records)):
                a_record = cur_date_records[i]
                if a_record.command in command_to_found:
                    last_day_complete_command_index = i
                    break
            if last_day_complete_command_index == -1:
                # unknown
                return node_situation
            submitted_index = last_day_complete_command_index + 1
            if cur_date_records[submitted_index].command != 'submitted':
                # unknown
                return node_situation
            submitted_time = datetime.datetime.combine(cur_date_records[submitted_index].date,
                                                       cur_date_records[submitted_index].time)

            # find this day complete
            # we should have next date records in record list.
            if len(next_date_records) == 0:
                return node_situation

            # find complete
            command_to_found = ['complete', 'aborted']
            this_day_command_index = -1
            for i in range(0, len(next_date_records)):
                a_record = next_date_records[i]
                if a_record.command in command_to_found:
                    this_day_command_index = i
                    break
            if this_day_command_index == -1:
                # unknown
                return node_situation

            # error
            if next_date_records[this_day_command_index].command == "aborted":
                node_situation['situation'] = 'error'
                return node_situation

            # normal
            complete_index = this_day_command_index
            complete_time = datetime.datetime.combine(next_date_records[complete_index].date,
                                                      next_date_records[complete_index].time)

            if 'time_point' not in node_situation:
                    node_situation['time_point'] = {}
            node_situation['time_point']['submitted'] = submitted_time
            node_situation['time_point']['complete'] = complete_time

            time_in_all = complete_time - submitted_time
            if 'time_period' not in node_situation:
                    node_situation['time_period'] = {}
            node_situation['time_period']['in_all'] = {
                'days': time_in_all.days,
                'seconds': time_in_all.seconds
            }
            node_situation['situation'] = 'normal'

        return node_situation
