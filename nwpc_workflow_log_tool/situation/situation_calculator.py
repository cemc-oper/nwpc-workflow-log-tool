import typing
import datetime

from loguru import logger
import pandas as pd

from nwpc_workflow_log_model.log_record.ecflow import StatusLogRecord
from nwpc_workflow_log_model.log_record.ecflow.status_record import StatusChangeEntry

from nwpc_workflow_log_tool.util import generate_in_date_range, generate_later_than_time, print_records
from .situation_record import SituationRecord


class SituationCalculator(object):
    """
    计算节点的运行状态 `NodeSituation`

    Attributes
    ----------
    _dfa_engine:
        用于计算节点运行状态的DFA类
    _stop_states: typing.Tuple
        停止计算DFA的运行状态
    _dfa_kwargs: dict
        创建DFA时的附加参数
    """
    def __init__(
            self,
            dfa_engine,
            stop_states: typing.Tuple,
            dfa_kwargs: dict = None,
    ):
        self._dfa_engine = dfa_engine
        self._stop_states = stop_states
        self._dfa_kwargs = dfa_kwargs
        if self._dfa_kwargs is None:
            self._dfa_kwargs = dict()

    def get_situations(
            self,
            records: typing.List,
            node_path: str,
            start_date: datetime.datetime,
            end_date: datetime.datetime,
            earliest_time: datetime.time = None,
    ) -> typing.List[SituationRecord]:
        """
        Get situations for some node in date range [start_date, end_date).

        Parameters
        ----------
        records
        node_path
        start_date
        end_date
        earliest_time: datetime.time
            If ``earliest_time`` is set, only records after earliest_time for some date is used.
            This options is mainly for nodes which run over midnight.

        Returns
        -------
        typing.List[SituationRecord]

        """
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
            if earliest_time is not None:
                filter_function = generate_later_than_time(current_date, earliest_time)
                current_records = list(filter(lambda x: filter_function(x), current_records))

            status_changes = [StatusChangeEntry(r) for r in current_records]

            dfa = self._dfa_engine(
                name=current_date,
                **self._dfa_kwargs,
            )

            for s in status_changes:
                dfa.trigger(
                    s.status.value,
                    node_data=s,
                )
                if dfa.state in self._stop_states:
                    break

            situations.append(SituationRecord(
                date=current_date,
                state=dfa.state,
                node_situation=dfa.node_situation,
                records=current_records,
            ))

        logger.info("Calculating node status change using DFA...Done")
        return situations
