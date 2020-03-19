import typing
from loguru import logger
import pandas as pd
from scipy import stats

from nwpc_workflow_log_model.analytics.node_situation import (
    NodeStatus,
)
from nwpc_workflow_log_model.analytics.situation_type import (
    FamilySituationType,
    TaskSituationType
)

from nwpc_workflow_log_tool.situation.situation_record import SituationRecord

from .presenter import Presenter


class StatusPresenter(Presenter):
    def __init__(
            self,
            target_node_status: NodeStatus,
            target_state: FamilySituationType or TaskSituationType
    ):
        super(StatusPresenter, self).__init__()
        self.target_node_status = target_node_status
        self.target_state = target_state

    def present(self, situations: typing.Iterable[SituationRecord]):
        time_series = []
        for a_situation in situations:
            current_date = a_situation.date
            current_records = a_situation.records
            if a_situation.state is self.target_state:
                node_situation = a_situation.node_situation
                time_points = node_situation.time_points
                point = next((i for i in time_points if i.status == self.target_node_status), None)
                if point is None:
                    logger.warning("[{}] skip: no time point {}",
                                   current_date.strftime("%Y-%m-%d"),
                                   self.target_node_status)
                    # print_records(current_records)
                else:
                    time_length = point.time - current_date
                    time_series.append(time_length)
                    logger.info("[{}] {}", current_date.strftime("%Y-%m-%d"), time_length)
            else:
                logger.warning("[{}] skip: DFA is not in complete", current_date.strftime("%Y-%m-%d"))
                # print_records(current_records)

        time_series = pd.Series(time_series)
        time_series_mean = time_series.mean()
        print()
        print("Mean:")
        print(time_series_mean)

        ratio = 0.25
        time_series_trim_mean = stats.trim_mean(time_series.values, ratio)
        print()
        print(f"Trim Mean ({ratio}):")
        print(pd.to_timedelta(time_series_trim_mean))
