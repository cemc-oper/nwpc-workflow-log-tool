from nwpc_log_model.util.version_util import get_version

if __name__ == "__main__":
    file_path = "/vagrant_data/sms-log-sample/nwp/cma20n03.sms.log"
    with open(file_path) as f:
        first_line = f.readline().strip()
        print(first_line)
        version = get_version('nwp_xp', 'nwpc_op', first_line)
        print(version)
