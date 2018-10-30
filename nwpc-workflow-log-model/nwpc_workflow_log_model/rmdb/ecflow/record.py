# coding: utf-8
from datetime import datetime
from sqlalchemy import Column, Integer, String, Date, Time, Text


class EcflowRecord(object):
    repo_id = Column(Integer, primary_key=True)
    version_id = Column(Integer, primary_key=True)
    line_no = Column(Integer, primary_key=True)
    log_type = Column(String(10))
    date = Column(Date())
    time = Column(Time())
    command_type = Column(String(10))
    command = Column(String(100))
    node_path = Column(String(200))
    additional_information = Column(Text())
    log_record = Column(Text())

    @classmethod
    def parse(cls, line):
        record = EcflowRecord()
        record.log_record = line

        start_pos = 0
        end_pos = line.find(':')
        record.log_type = line[start_pos:end_pos]

        start_pos = end_pos + 2
        end_pos = line.find(']', start_pos)
        if end_pos == -1:
            print("can't find date and time => ", line)
            return
        record_time_string = line[start_pos:end_pos]
        date_time = datetime.strptime(record_time_string, '%H:%M:%S %d.%m.%Y')
        record.date = date_time.date()
        record.time = date_time.time()

        start_pos = end_pos + 2
        if line[start_pos: start_pos+1] == " ":
            record.command_type = "status"
            start_pos += 1
            record.parse_status_record(line[start_pos:])
        elif line[start_pos: start_pos+2] == "--":
            record.command_type = "client"
            # print("[client command]", line)
        elif line[start_pos: start_pos+4] == "chd:":
            # child
            record.command_type = "child"
            start_pos += 4
            record.parse_client_record(line[start_pos:])
        elif line[start_pos: start_pos+4] == "svr:":
            # server
            # print("[server command]", line)
            record.command_type = "server"
        else:
            # not supported
            print("[not supported]", line)
        return record

        if self.record_command == 'alter':
            start_pos = end_pos+1
            pos = line.find(' [', start_pos)
            if pos != -1:
                self.record_fullname = line[start_pos:pos]
                start_pos = pos + 2
                end_pos = line.find('] ', start_pos)
                if end_pos != -1:
                    self.record_additional_information = line[start_pos:end_pos]

        elif self.record_command == 'meter':
            if self.record_type != 'ERR':
                start_pos = end_pos + 1
                end_pos = line.find(' ', start_pos)
                self.record_fullname = line[start_pos:end_pos]
                start_pos = end_pos + 4
                self.record_additional_information = line[start_pos:]
            else:
                start_pos = end_pos + 1
                if line[start_pos] == "/":
                    # ERR:[12:52:00 19.5.2014] meter:/gmf_gsi_v1r5/T639/06/dasrefresh:WaitingMins: 55 out of range [0 - 40]
                    end_pos = line.find(' ', start_pos)
                    if end_pos != -1 and line[end_pos-1] == ":":
                        self.record_fullname = line[start_pos: end_pos-1]
                        self.record_additional_information = line[end_pos+1:]
                else:
                    # ERR:[03:41:58 10.7.2014] meter:WaitingMins for /grapes_meso_v4_0/cold/00/pre_data/obs_get/aob_get:the node was not found:
                    pass

        elif self.record_command in ['begin', 'autorepeat date']:
            start_pos = end_pos + 1
            end_pos = line.find(' ', start_pos)
            self.record_fullname = line[start_pos: end_pos]

        elif self.record_command == 'force' or self.record_command == 'force(recursively)':
            start_pos = end_pos + 1
            end_pos = line.find(' ', start_pos)
            self.record_fullname = line[start_pos:end_pos]
            if line[end_pos:end_pos+4] == " to ":
                start_pos = end_pos + 4
                end_pos = line.find(' ', start_pos)
                self.record_additional_information = line[start_pos:end_pos]

        elif self.record_command == 'delete':
            start_pos = end_pos + 1
            end_pos = line.find(' ', start_pos)
            self.record_fullname = line[start_pos:end_pos]
            start_pos = end_pos + 1
            end_pos = line.find(' ', start_pos)
            self.record_additional_information = line[start_pos:end_pos]

        elif self.record_command in ['set', 'clear']:
            start_pos = end_pos + 1
            end_pos = line.find(':', start_pos)
            if end_pos != -1:
                self.record_fullname = line[start_pos: end_pos]
                self.record_additional_information = line[end_pos+1:]

        # WAR:[23:37:08 16.8.2015] requeue:/gmf_grapes_v1423/grapes_global/12/post/postp_084 from aborted
        # MSG:[23:37:08 16.8.2015] requeue:user nwp@5986333:/gmf_grapes_v1423/grapes_global/12/post/postp_084
        elif self.record_command == 'requeue':
            start_pos = end_pos + 1
            if self.record_type == "WAR":
                end_pos = line.find(' ', start_pos)
                if end_pos != -1:
                    self.record_fullname = line[start_pos: end_pos]
                    start_pos = end_pos + 1
                    self.record_additional_information = line[start_pos:]
            elif self.record_type == "MSG":
                end_pos = line.find(':', start_pos)
                if end_pos != -1:
                    self.record_additional_information = line[start_pos: end_pos]
                    start_pos = end_pos + 1
                    self.record_fullname = line[start_pos:]

    def parse_status_record(self, status_line):
        """
        active: /swfdp/00/deterministic/base/024/SWFDP_CA/CIN_SWFDP_CA_sep_024
        """
        start_pos = 0
        end_pos = status_line.find(":", start_pos)
        if end_pos == -1:
            print("[ERROR] status record: command not found =>", self.log_record)
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
                # LOG:[11:09:31 20.9.2018]  aborted: /grapes_meso_3km_post/06/tograph/3h/prep_3h_10mw/plot_hour_030 try-no: 1 reason: trap
                self.node_path = status_line[start_pos:end_pos]
                self.additional_information = status_line[end_pos+1:]
        else:
            if command in ('unknown', ):
                # just ignore
                pass
            elif ' ' in command:
                print("[ERROR] status record: is not a valid command =>", self.log_record)
            else:
                self.command = command
                print("[ERROR] status record: command not supported =>", self.log_record)

    def parse_client_record(self, client_line):
        start_pos = 0
        end_pos = client_line.find(" ", start_pos)
        if end_pos == -1:
            print("[ERROR] client record: command not found =>", self.log_record)
            return
        command = client_line[start_pos:end_pos]

        if command in ('init', 'complete', 'abort'):
            self.command = command
            start_pos = end_pos + 2
            end_pos = client_line.find(' ', start_pos)
            if end_pos == -1:
                # MSG:[08:17:04 29.6.2018] chd:complete /gmf_grapes_025L60_v2.2_post/18/typhoon/post/tc_post
                self.node_path = client_line[start_pos:].strip()
            else:
                # MSG:[12:22:53 19.10.2018] chd:abort /3km_post/06/3km_togrib2/grib2WORK/030/after_data2grib2_030  trap
                self.node_path = client_line[start_pos:end_pos]
                self.additional_information = client_line[end_pos + 1:]
        elif command in ('meter', 'label', 'event'):
            self.command = command
            # MSG:[09:24:06 29.6.2018] chd:event transmissiondone /gmf_grapes_025L60_v2.2_post/00/tograph/base/015/AN_AEA/QFLXDIV_P700_AN_AEA_sep_015
            self.command = command
            start_pos = end_pos + 1
            line = client_line[start_pos:]
            node_path_start_pos = line.rfind(' ')
            if node_path_start_pos != -1:
                self.node_path = line[node_path_start_pos+1:]
                self.additional_information = line[:node_path_start_pos]
            else:
                print("[ERROR] client record: parse error =>", self.log_record)
        else:
            self.command = command
            print("[ERROR] client record: command not supported =>", self.log_record)
