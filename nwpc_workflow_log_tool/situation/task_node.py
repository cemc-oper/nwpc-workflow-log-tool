import datetime
import typing

from loguru import logger

from nwpc_workflow_model.node_status import NodeStatus
from nwpc_workflow_log_model.analytics.task_status_change_dfa import (
    TaskStatusChangeDFA,
    TaskSituationType,
)
from nwpc_workflow_log_collector.ecflow.log_file_util import get_record_list

from nwpc_workflow_log_tool.presenter import StatusPresenter

from .situation_calculator import SituationCalculator


def analytics_task_node_log_with_status(
        file_path: str,
        node_path: str,
        node_status: NodeStatus,
        start_date: datetime.datetime,
        stop_date: datetime.datetime,
        verbose: int,
):
    logger.info(f"Analytic time points")
    logger.info(f"\tnode_path: {node_path}")
    logger.info(f"\tnode_status: {node_status}")
    logger.info(f"\tstart_date: {start_date}")
    logger.info(f"\tend_date: {stop_date}")

    logger.info(f"Getting log lines...")
    records = get_record_list(file_path, node_path, start_date, stop_date)
    logger.info(f"Getting log lines...Done, {len(records)} lines")

    calculator = SituationCalculator(
        dfa_engine=TaskStatusChangeDFA,
        stop_states=(
            TaskSituationType.Complete,
        )
    )

    situations = calculator.get_situations(
        records=records,
        node_path=node_path,
        start_date=start_date,
        end_date=stop_date,
    )

    presenter = StatusPresenter(
        target_node_status=node_status,
        target_state=TaskSituationType.Complete,
    )
    presenter.present(situations)
