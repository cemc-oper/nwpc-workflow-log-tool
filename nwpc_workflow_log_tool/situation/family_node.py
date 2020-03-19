import datetime
import typing

from loguru import logger

from nwpc_workflow_model.node_status import NodeStatus
from nwpc_workflow_log_model.analytics.family_status_change_dfa import (
    FamilyStatusChangeDFA,
    FamilySituationType,
)
from nwpc_workflow_log_collector.ecflow.log_file_util import get_record_list

from nwpc_workflow_log_tool.presenter import StatusPresenter
from .situation_calculator import SituationCalculator


def analytics_family_node_log_with_status(
        file_path: str,
        node_path: str,
        node_status: NodeStatus,
        start_date: datetime.datetime,
        stop_date: datetime.datetime,
        verbose: int,
):
    logger.info(f"Analytic time points for family node")
    logger.info(f"\tnode_path: {node_path}")
    logger.info(f"\tnode_status: {node_status}")
    logger.info(f"\tstart_date: {start_date}")
    logger.info(f"\tstop_date: {stop_date}")

    logger.info(f"Getting log lines...")
    records = get_record_list(file_path, node_path, start_date, stop_date)
    logger.info(f"Getting log lines...Done, {len(records)} lines")

    calculator = SituationCalculator(
        dfa_engine=FamilyStatusChangeDFA,
        stop_states=(
            FamilySituationType.Complete,
            FamilySituationType.Error,
        ),
        dfa_kwargs={
            "ignore_aborted": True,
        }
    )

    situations = calculator.get_situations(
        records=records,
        node_path=node_path,
        start_date=start_date,
        end_date=stop_date,
    )

    presenter = StatusPresenter(
        target_node_status=node_status,
        target_state=FamilySituationType.Complete,
    )
    presenter.present(situations)
