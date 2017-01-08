from nwpc_log_collector.file_collector import collect_log_from_local_file
import datetime

if __name__ == "__main__":
    start_time = datetime.datetime.now()

    file_path = "/vagrant_data/sms-log-sample/nwp_qu/cma20n03.sms.log"
    collect_log_from_local_file('nwp_xp', 'nwpc_qu', file_path)

    end_time = datetime.datetime.now()
    print(end_time - start_time)
