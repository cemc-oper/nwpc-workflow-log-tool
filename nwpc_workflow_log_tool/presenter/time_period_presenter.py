import pandas as pd
from scipy import stats

from nwpc_workflow_log_model.analytics.situation_type import (
    FamilySituationType,
    TaskSituationType
)

from .presenter import Presenter


class TimePeriodPresenter(Presenter):
    """
    输出时间段内给定的节点状态（NodeStatus）时间段，并计算开始时间和结束时间的切尾均值（0.25）

    Attributes
    ----------
    target_state: FamilySituationType or TaskSituationType
        节点运行状态，只计算符合该状态的节点。一般只关心正常结束的节点，所以常用值为
            - `TaskSituationType.Complete`
            - `FamilySituationType.Complete`
    """
    def __init__(
            self,
            target_state: FamilySituationType or TaskSituationType
    ):
        super(TimePeriodPresenter, self).__init__()
        self.target_state = target_state

    def present(self, table_data: pd.DataFrame):
        if "time_period_in_all" not in table_data:
            raise ValueError("time_period_in_all is not in table_data")

        table_data["start_clock"] = table_data.time_period_in_all_start - table_data.start_time
        table_data["end_clock"] = table_data.time_period_in_all_end - table_data.start_time

        table_data.rename(columns={
            "time_period_in_all": "duration"
        }, inplace=True)

        with pd.option_context("display.max_rows", None, "display.max_columns", None):
            print(table_data[["start_time", "start_clock", "end_clock", "duration"]])

        ratio = 0.25

        time_series_trim_mean = stats.trim_mean(table_data["start_clock"].values, ratio)
        print()
        print(f"Trimmed Mean for start time ({ratio}):")
        print(pd.to_timedelta(time_series_trim_mean))

        time_series_trim_mean = stats.trim_mean(table_data["end_clock"].values, ratio)
        print()
        print(f"Trimmed Mean for end time ({ratio}):")
        print(pd.to_timedelta(time_series_trim_mean))
