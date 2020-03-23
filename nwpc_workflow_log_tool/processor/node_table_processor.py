import typing
from collections import defaultdict

import pandas as pd
from loguru import logger

from nwpc_workflow_log_model.analytics.situation_type import (
    FamilySituationType,
    TaskSituationType
)

from .processor import Processor, SituationRecord


class NodeTableProcessor(Processor):
    """
    将节点运行状态转成 ``pandas.DataFrame`` 表格数据

    Attributes
    ----------
    node_path: str
        节点路径
    target_state: TaskSituationType or FamilySituationType
        有效记录对应的运行状态
    columns:
        表格列名称
    """
    def __init__(
            self,
            node_path: str,
            target_state: TaskSituationType or FamilySituationType,
    ):
        super(NodeTableProcessor, self).__init__()
        self.node_path = node_path
        self.target_state = target_state
        self.columns = [
            "start_time",
            "situation",
            "time_point_submitted",
            "time_point_active",
            "time_point_complete",
            "time_point_aborted",
            "time_period_in_all",
            "time_period_in_all_start",
            "time_period_in_all_end",
            "time_period_in_submitted",
            "time_period_in_submitted_start",
            "time_period_in_submitted_end",
            "time_period_in_active",
            "time_period_in_active_start",
            "time_period_in_active_end",
        ]

    def process(self, situations: typing.Iterable[SituationRecord]) -> pd.DataFrame:
        df = pd.DataFrame(columns=self.columns)
        for a_situation in situations:
            current_date = a_situation.date
            current_columns = defaultdict(list)
            current_columns["start_time"] = [current_date]
            current_columns["situation"] = [a_situation.state.name]
            if a_situation.state is self.target_state:
                node_situation = a_situation.node_situation
                time_points = node_situation.time_points
                for time_point in time_points:
                    status = time_point.status.name
                    key = f"time_point_{status.lower()}"
                    if key in self.columns:
                        current_columns[key] = [time_point.time]

                for time_period in node_situation.time_periods:
                    period_name = time_period.period_type.value
                    key = f"time_period_{period_name}"
                    if key in self.columns:
                        current_columns[key] = [pd.Timedelta(time_period.end_time - time_period.start_time)]
                        current_columns[key + "_start"] = [pd.Timestamp(time_period.start_time)]
                        current_columns[key + "_end"] = [pd.Timestamp(time_period.start_time)]

                current_df = pd.DataFrame(
                    current_columns,
                    columns=self.columns,
                    index=[f"{current_date.strftime('%Y%m%d%H')}"]
                )
                df = df.append(current_df)
            else:
                current_df = pd.DataFrame(
                    current_columns,
                    columns=self.columns,
                    index=[f"{current_date.strftime('%Y%m%d%H')}"]
                )
                df = df.append(current_df)
                logger.warning("[{}] skip: DFA is not in complete", current_date.strftime("%Y-%m-%d"))
        df.sort_index(inplace=True)
        return df
