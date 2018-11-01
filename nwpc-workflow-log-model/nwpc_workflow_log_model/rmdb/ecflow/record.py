# coding: utf-8
from datetime import datetime

from sqlalchemy import Column, String, Index
from nwpc_workflow_log_model.rmdb.base.model import Model
from nwpc_workflow_log_model.rmdb.base.record import RecordBase


class EcflowRecordBase(RecordBase):
    command_type = Column(String(10))

    def parse(self, line):
        self.log_record = line

        start_pos = 0
        end_pos = line.find(':')
        self.log_type = line[start_pos:end_pos]

        start_pos = end_pos + 2
        end_pos = line.find(']', start_pos)
        if end_pos == -1:
            print("can't find date and time => ", line)
            return
        record_time_string = line[start_pos:end_pos]
        date_time = datetime.strptime(record_time_string, '%H:%M:%S %d.%m.%Y')
        self.date = date_time.date()
        self.time = date_time.time()

        start_pos = end_pos + 2
        if line[start_pos: start_pos+1] == " ":
            self.command_type = "status"
            start_pos += 1
            self.__parse_status_record(line[start_pos:])
        elif line[start_pos: start_pos+2] == "--":
            self.command_type = "client"
            start_pos += 2
            self.__parse_client_record(line[start_pos:])
        elif line[start_pos: start_pos+4] == "chd:":
            # child
            self.command_type = "child"
            start_pos += 4
            self.__parse_child_record(line[start_pos:])
        elif line[start_pos: start_pos+4] == "svr:":
            # server
            # print("[server command]", line)
            self.command_type = "server"
        elif line[start_pos:].strip()[0].isupper():
            # WAR:[09:00:08 6.8.2018] Job generation for task /grapes_emer_v1_1/00/plot/get_plot/get_plot_meso
            #  took 4593ms, Exceeds ECF_TASK_THRESHOLD(4000ms)
            pass
        else:
            # not supported
            # print("[not supported]", line)
            pass

        return self

    def __parse_status_record(self, status_line):
        """
        active: /swfdp/00/deterministic/base/024/SWFDP_CA/CIN_SWFDP_CA_sep_024
        """
        start_pos = 0
        end_pos = status_line.find(":", start_pos)
        if end_pos == -1:
            if status_line.strip()[0].isupper():
                pass
            else:
                # print("[ERROR] status record: command not found =>", self.log_record)
                pass
            return
        command = status_line[start_pos:end_pos]

        if command in ('submitted', 'active', 'queued', 'complete', 'aborted'):
            self.command = command
            start_pos = end_pos + 2
            end_pos = status_line.find(' ', start_pos)
            if end_pos == -1:
                # LOG:[23:12:00 9.10.2018] queued: /grapes_meso_3km_post/18/tograph/1h/prep_1h_10mw
                self.node_path = status_line[start_pos:].strip()
            else:
                # LOG:[11:09:31 20.9.2018]  aborted: /grapes_meso_3km_post/06/tograph/3h/prep_3h_10mw/plot_hour_030
                #  try-no: 1 reason: trap
                self.node_path = status_line[start_pos:end_pos]
                self.additional_information = status_line[end_pos+1:]
        else:
            if command in ('unknown', ):
                # just ignore
                pass
            elif command.strip()[0].isupper():
                pass
            elif command[0] == '[':
                # WAR:[09:16:14 8.8.2018]  [ overloaded || --abort*2 ] (pid & password match) : chd:abort
                #  : /grapes_emer_v1_1/12/plot/plot_wind : already aborted : action(fob)
                pass
            else:
                self.command = command
                # print("[ERROR] status record: command not supported =>", self.log_record)

    def __parse_child_record(self, child_line):
        start_pos = 0
        end_pos = child_line.find(" ", start_pos)
        if end_pos == -1:
            # print("[ERROR] child record: command not found =>", self.log_record)
            return
        command = child_line[start_pos:end_pos]

        if command in ('init', 'complete', 'abort'):
            self.command = command
            start_pos = end_pos + 2
            end_pos = child_line.find(' ', start_pos)
            if end_pos == -1:
                # MSG:[08:17:04 29.6.2018] chd:complete /gmf_grapes_025L60_v2.2_post/18/typhoon/post/tc_post
                self.node_path = child_line[start_pos:].strip()
            else:
                # MSG:[12:22:53 19.10.2018] chd:abort
                #  /3km_post/06/3km_togrib2/grib2WORK/030/after_data2grib2_030  trap
                self.node_path = child_line[start_pos:end_pos]
                self.additional_information = child_line[end_pos + 1:]
        elif command in ('meter', 'label', 'event'):
            self.command = command
            # MSG:[09:24:06 29.6.2018] chd:event transmissiondone
            #  /gmf_grapes_025L60_v2.2_post/00/tograph/base/015/AN_AEA/QFLXDIV_P700_AN_AEA_sep_015
            self.command = command
            start_pos = end_pos + 1
            line = child_line[start_pos:]
            node_path_start_pos = line.rfind(' ')
            if node_path_start_pos != -1:
                self.node_path = line[node_path_start_pos+1:]
                self.additional_information = line[:node_path_start_pos]
            else:
                # print("[ERROR] child record: parse error =>", self.log_record)
                pass
        else:
            self.command = command
            print("[ERROR] child record: command not supported =>", self.log_record)

    def __parse_client_record(self, child_line):
        start_pos = 0
        end_pos = child_line.find(" ", start_pos)
        if end_pos == -1:
            # print("[ERROR] client record: command not found =>", self.log_record)
            return
        command = child_line[start_pos:end_pos]

        if command == 'requeue':
            self.command = command
            start_pos = end_pos + 1
            tokens = child_line[start_pos:].split()
            if len(tokens) == 3:
                requeue_option = tokens[0]
                node_path = tokens[1]
                user = tokens[2]
                self.node_path = node_path
                self.additional_information = requeue_option + ' ' + user
            else:
                # print("[ERROR] client record: requeue parse error =>", self.log_record)
                return
        elif command in ('alter', 'free-dep', 'kill', 'delete', 'suspend', 'resume', 'run', 'status'):
            self.command = command
            start_pos = end_pos + 1
            tokens = child_line[start_pos:].split()
            user = tokens[-1]
            node_path = tokens[-2]
            self.node_path = node_path
            self.additional_information = ' '.join(tokens[:-2]) + ' ' + user
        elif command.startswith('force='):
            self.command = 'force'
            start_pos = end_pos + 1
            tokens = child_line[start_pos:].split()
            node_path = tokens[-2]
            user = tokens[-1]
            self.node_path = node_path
            self.additional_information = ' '.join(tokens[-2:]) + ' ' + user
        elif command.startswith('file='):
            self.command = 'file'
            node_path = command[5:]
            self.node_path = node_path
            start_pos = end_pos + 1
            self.additional_information = child_line[start_pos:]
        elif command.startswith('load='):
            self.command = 'load'
            node_path = command[5:]
            self.node_path = node_path
            start_pos = end_pos + 1
            self.additional_information = child_line[start_pos:]
        elif command.startswith('begin='):
            self.command = 'begin'
            node_path = command[6:]
            self.node_path = node_path
            start_pos = end_pos + 1
            self.additional_information = child_line[start_pos:]
        elif command.startswith('replace='):
            self.command = 'replace'
            node_path = command[5:]
            self.node_path = node_path
            start_pos = end_pos + 1
            self.additional_information = child_line[start_pos:]
        elif command.startswith('order='):
            self.command = 'order'
            node_path = command[6:]
            self.node_path = node_path
            start_pos = end_pos + 1
            self.additional_information = child_line[start_pos:]
        elif command in ('restart', 'suites', 'stats', 'edit_history',
                         'zombie_get', 'server_version', 'ping', 'check_pt'):
            self.command = command
        elif command.startswith('sync_full=') or \
                command.startswith('news=') or \
                command.startswith('sync=') or \
                command.startswith('edit_script=') or \
                command.startswith('zombie_fail=') or \
                command.startswith('zombie_kill=') or \
                command.startswith('zombie_fob=') or \
                command.startswith('zombie_adopt=') or \
                command.startswith('zombie_remove=') or \
                command.startswith('log=') or \
                command.startswith('halt=') or \
                command.startswith('terminate=') or \
                command.startswith('order=') or \
                command.startswith('ch_register=') or \
                command.startswith('ch_drop='):
            self.command = command[:command.find('=')]
        else:
            self.command = command
            # print("[ERROR] client record: command not supported =>", self.log_record)


class EcflowRecord(EcflowRecordBase, Model):
    __tablename__ = "ecflow_record"
    owner = 'owner'
    repo = 'repo'

    def __init__(self):
        pass

    @classmethod
    def prepare(cls, owner, repo):
        """
        为 owner/repo 准备 Record 对象。当前需要修改 __tablename__ 为特定的表名。
        :param owner:
        :param repo:
        :return:
        """
        table_name = 'ecflow_record.{owner}.{repo}'.format(owner=owner, repo=repo)
        cls.__table__.name = table_name
        cls.owner = owner
        cls.repo = repo

        cls.__table_args__ = (
            Index('{owner}_{repo}_date_time_index'.format(owner=cls.owner, repo=cls.repo), 'date', 'time'),
            Index('{owner}_{repo}_command_index'.format(owner=cls.owner, repo=cls.repo), 'command'),
            Index('{owner}_{repo}_fullname_index'.format(owner=cls.owner, repo=cls.repo), 'node_path'),
            Index('{owner}_{repo}_type_index'.format(owner=cls.owner, repo=cls.repo), 'log_type')
        )

    @classmethod
    def init(cls):
        cls.__table__.name = 'ecflow_record'

        cls.owner = "owner"
        cls.repo = "repo"

        cls.__table_args__ = (
            Index('{owner}_{repo}_date_time_index'.format(owner=cls.owner, repo=cls.repo), 'date', 'time'),
            Index('{owner}_{repo}_command_index'.format(owner=cls.owner, repo=cls.repo), 'command'),
            Index('{owner}_{repo}_fullname_index'.format(owner=cls.owner, repo=cls.repo), 'node_path'),
            Index('{owner}_{repo}_type_index'.format(owner=cls.owner, repo=cls.repo), 'log_type')
        )

    @classmethod
    def create_record_table(cls, owner, repo, session):
        EcflowRecord.prepare(owner, repo)
        EcflowRecord.__table__.create(bind=session.get_bind(), checkfirst=True)
        EcflowRecord.init()
