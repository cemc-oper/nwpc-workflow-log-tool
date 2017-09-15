# coding: utf-8
import datetime


class SmsLogFileUtil(object):
    @staticmethod
    def get_line_no_range(log_file_path, begin_date, end_date):
        begin_line_no = 0
        end_line_no = 0
        with open(log_file_path) as log_file:
            cur_line = 0
            for line in log_file:
                if not line.startswith('#'):
                    # some line is :
                    # check emergency
                    # so, just ignore it.
                    continue
                cur_line += 1
                start_pos = 7
                end_pos = line.find(']', start_pos)
                time_string = line[start_pos:end_pos]
                date_time = datetime.datetime.strptime(time_string, '%H:%M:%S %d.%m.%Y')
                line_date = date_time.date()
                if line_date >= end_date:
                    return begin_line_no, end_line_no

                if line_date >= begin_date:
                    begin_line_no = cur_line
                    break
            else:
                return begin_line_no, end_line_no

            for line in log_file:
                cur_line += 1
                start_pos = 7
                end_pos = line.find(']', start_pos)
                time_string = line[start_pos:end_pos]
                date_time = datetime.datetime.strptime(time_string, '%H:%M:%S %d.%m.%Y')
                if date_time.date() >= end_date:
                    end_line_no = cur_line
                    break
            else:
                end_line_no = cur_line + 1

        return begin_line_no, end_line_no
