import datetime
import typing

import pandas as pd
from loguru import logger
from scipy import stats

from nwpc_workflow_model.node_status import NodeStatus
from nwpc_workflow_log_model.log_record.ecflow import StatusLogRecord
from nwpc_workflow_log_model.log_record.ecflow.status_record import StatusChangeEntry
from nwpc_workflow_log_model.analytics.task_status_change_dfa import (
    TaskStatusChangeDFA,
    TaskSituationType,
)
from nwpc_workflow_log_collector.ecflow.log_file_util import get_record_list

from nwpc_workflow_log_tool.util import generate_in_date_range, print_records
from nwpc_workflow_log_tool.presenter import StatusPresenter

from .situation_record import SituationRecord


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

    situations = get_task_node_situations(
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


def get_task_node_situations(
        records: list,
        node_path: str,
        start_date: datetime.datetime,
        end_date: datetime.datetime,
) -> typing.List[SituationRecord]:
    logger.info("Finding StatusLogRecord for {}", node_path)
    record_list = []
    for record in records:
        if record.node_path == node_path and isinstance(record, StatusLogRecord):
            record_list.append(record)

    logger.info("Calculating node status change using DFA...")
    situations = []
    for current_date in pd.date_range(start=start_date, end=end_date, closed="left"):
        filter_function = generate_in_date_range(current_date, current_date + pd.Timedelta(days=1))
        current_records = list(filter(lambda x: filter_function(x), record_list))

        status_changes = [StatusChangeEntry(r) for r in current_records]

        dfa = TaskStatusChangeDFA(name=current_date)

        for s in status_changes:
            dfa.trigger(
                s.status.value,
                node_data=s,
            )
            if dfa.state is TaskSituationType.Complete:
                break

        situations.append(SituationRecord(
            date=current_date,
            state=dfa.state,
            node_situation=dfa.node_situation,
            records=current_records,
        ))

    logger.info("Calculating node status change using DFA...Done")
    return situations
