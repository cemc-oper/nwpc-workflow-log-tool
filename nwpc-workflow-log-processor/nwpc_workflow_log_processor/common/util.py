# coding: utf-8
import yaml
from nwpc_workflow_model.ecflow import Bunch as EcflowBunch
from nwpc_workflow_model.sms import Bunch as SmsBunch

from nwpc_workflow_log_model.rmdb.sms.record import SmsRecord
from nwpc_workflow_log_model.rmdb.ecflow.record import EcflowRecord


def get_record_class(repo_type):
    if repo_type == "sms":
        return SmsRecord
    elif repo_type == "ecflow":
        return EcflowRecord
    else:
        raise ValueError(f"repo type is not supported: {repo_type}")


def get_bunch_class(repo_type: str):
    if repo_type == "ecflow":
        return EcflowBunch
    elif repo_type == "sms":
        return SmsBunch
    else:
        raise ValueError(f"repo_type is not supported: {repo_type}")


def load_config(config_file):
    with open(config_file) as f:
        config = yaml.safe_load(f)
        return config
