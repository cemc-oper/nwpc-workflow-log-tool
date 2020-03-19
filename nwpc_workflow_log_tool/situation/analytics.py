import datetime

from loguru import logger
from nwpc_workflow_log_collector.ecflow.log_file_util import get_record_list
from nwpc_workflow_log_model.analytics.family_status_change_dfa import FamilyStatusChangeDFA
from nwpc_workflow_log_model.analytics.situation_type import FamilySituationType, TaskSituationType
from nwpc_workflow_log_model.analytics.task_status_change_dfa import TaskStatusChangeDFA
from nwpc_workflow_model.node_status import NodeStatus

from nwpc_workflow_log_tool.presenter import StatusPresenter
from nwpc_workflow_log_tool.situation import SituationCalculator


def analytics_node_log_with_status(
        node_type: str,
        file_path: str,
        node_path: str,
        node_status: NodeStatus,
        start_date: datetime.datetime,
        stop_date: datetime.datetime,
        verbose: int,
):
    logger.info(f"Analytic time points for {node_type} node")
    logger.info(f"\tnode_path: {node_path}")
    logger.info(f"\tnode_status: {node_status}")
    logger.info(f"\tstart_date: {start_date}")
    logger.info(f"\tstop_date: {stop_date}")

    if node_type == "family":
        dfa_engine = FamilyStatusChangeDFA
        stop_states = (
            FamilySituationType.Complete,
            FamilySituationType.Error,
        )
        dfa_kwargs = {
            "ignore_aborted": True
        }
        target_state = FamilySituationType.Complete
    elif node_type == "task":
        dfa_engine = TaskStatusChangeDFA
        stop_states = (
            TaskSituationType.Complete,
        )
        dfa_kwargs = None
        target_state = TaskSituationType.Complete
    else:
        raise NotImplemented(f"node type is not supported: {node_type}")

    logger.info(f"Getting log lines...")
    records = get_record_list(file_path, node_path, start_date, stop_date)
    logger.info(f"Getting log lines...Done, {len(records)} lines")

    calculator = SituationCalculator(
        dfa_engine=dfa_engine,
        stop_states=stop_states,
        dfa_kwargs=dfa_kwargs,
    )

    situations = calculator.get_situations(
        records=records,
        node_path=node_path,
        start_date=start_date,
        end_date=stop_date,
    )

    presenter = StatusPresenter(
        target_node_status=node_status,
        target_state=target_state,
    )
    presenter.present(situations)
