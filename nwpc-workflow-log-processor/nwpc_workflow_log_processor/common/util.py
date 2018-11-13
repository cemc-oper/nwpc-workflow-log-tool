# coding: utf-8
from nwpc_workflow_log_model.rmdb.sms.record import SmsRecord
from nwpc_workflow_log_model.rmdb.ecflow.record import EcflowRecord


def get_record_class(repo_type):
    if repo_type == "sms":
        record_class = SmsRecord
    elif repo_type == "ecflow":
        record_class = EcflowRecord
    else:
        raise ValueError("repo type is not supported: " + repo_type)
    return record_class
