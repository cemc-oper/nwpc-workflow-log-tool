import pandas as pd
from scipy import stats

from nwpc_workflow_log_model.analytics.node_situation import (
    NodeStatus,
)
from nwpc_workflow_log_model.analytics.situation_type import (
    FamilySituationType,
    TaskSituationType
)

from .presenter import Presenter


class TimePointPresenter(Presenter):
    """
    输出时间段内给定的节点状态（NodeStatus）时间点，并计算均值和切尾均值（0.25）

    Notes
    -----
    因为DFA算法无法识别某些特殊情况，平均值一般不具有实际意义，请使用切尾均值。

    Attributes
    ----------
    target_node_status: NodeStatus
        目标状态，常用值：
            - `NodeStatus.Complete` 指示节点处于完成状态
            - `NodeStatus.Submitted` 指示节点处于提交状态

    target_state: FamilySituationType or TaskSituationType
        节点运行状态，只计算符合该状态的节点。一般只关心正常结束的节点，所以常用值为
            - `TaskSituationType.Complete`
            - `FamilySituationType.Complete`
    """
    def __init__(
            self,
            target_node_status: NodeStatus,
            target_state: FamilySituationType or TaskSituationType
    ):
        super(TimePointPresenter, self).__init__()
        self.target_node_status = target_node_status
        self.target_state = target_state

    def present(self, table_data: pd.DataFrame):
        key = f"time_point_{self.target_node_status.name}"
        if key not in table_data:
            raise ValueError(f"{key} is not in table data")

        time_series = table_data[key] - table_data.start_time
        time_series_mean = time_series.mean()
        print()
        print("Mean:")
        print(time_series_mean)

        ratio = 0.25
        time_series_trim_mean = stats.trim_mean(time_series.values, ratio)
        print()
        print(f"Trim Mean ({ratio}):")
        print(pd.to_timedelta(time_series_trim_mean))
