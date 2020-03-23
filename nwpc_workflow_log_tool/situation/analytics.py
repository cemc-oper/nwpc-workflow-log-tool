import datetime

from loguru import logger

from nwpc_workflow_model.node_status import NodeStatus
from nwpc_workflow_log_model.analytics.situation_type import FamilySituationType, TaskSituationType
from nwpc_workflow_log_model.analytics.task_status_change_dfa import TaskStatusChangeDFA
from nwpc_workflow_log_model.analytics.family_status_change_dfa import FamilyStatusChangeDFA
from nwpc_workflow_log_collector.ecflow.log_file_util import get_record_list

from nwpc_workflow_log_tool.presenter import TimePointPresenter
from nwpc_workflow_log_tool.situation import SituationCalculator
from nwpc_workflow_log_tool.processor import NodeTableProcessor


def analytics_time_point_with_status(
        node_type: str,
        file_path: str,
        node_path: str,
        node_status: NodeStatus,
        start_date: datetime.datetime,
        stop_date: datetime.datetime,
        verbose: int = 1,
):
    """
    从日志文件中获取一定时间范围([`start_date`, `stop_date`))内某节点进入某状态(`NodeStatus`)的时间点，
    并计算均值和切尾均值。

    Parameters
    ----------
    node_type: str
        节点类型。
        - `family`: 容器节点
        - `task`: 任务节点
    file_path: str
        日志文件路径。推荐预先使用`grep`等工具从日志文件中提取与节点路径相关的日志条目，节省解析文件时间。
    node_path: str
        节点路径
    node_status: NodeStatus
        节点状态，例如 `NodeStatus.complete` 和 `NodeStatus.submitted` 等
    start_date: datetime.datetime
        起始时间，[`start_date`, `stop_date`)
    stop_date: datetime.datetime
        结束日期，不包括在内 ,[`start_date`, `stop_date`)
    verbose: int
        输出级别，尚未实装
    Returns
    -------
    None

    Examples
    --------
    >>> analytics_time_point_with_status(
    ...     node_type="task",
    ...     file_path="ecflow.log",
    ...     node_path="/grapes_meso_3km_v4_4/00/model/fcst",
    ...     node_status=NodeStatus.complete,
    ...     start_date=datetime.datetime(2020, 3, 10),
    ...     end_date=datetime.datetime(2020, 3, 17),
    ... )
    # 省略部分输出
    2020-03-20 01:22:45.849 | INFO     | nwpc_workflow_log_tool.presenter.time_point_presenter:present:61 - [2020-03-10] 0 days 05:50:38
    2020-03-20 01:22:45.849 | INFO     | nwpc_workflow_log_tool.presenter.time_point_presenter:present:61 - [2020-03-11] 0 days 06:00:40
    2020-03-20 01:22:45.849 | INFO     | nwpc_workflow_log_tool.presenter.time_point_presenter:present:61 - [2020-03-12] 0 days 05:50:06
    2020-03-20 01:22:45.849 | INFO     | nwpc_workflow_log_tool.presenter.time_point_presenter:present:61 - [2020-03-13] 0 days 06:22:54
    2020-03-20 01:22:45.849 | INFO     | nwpc_workflow_log_tool.presenter.time_point_presenter:present:61 - [2020-03-14] 0 days 05:44:59
    2020-03-20 01:22:45.849 | INFO     | nwpc_workflow_log_tool.presenter.time_point_presenter:present:61 - [2020-03-15] 0 days 06:15:17
    2020-03-20 01:22:45.849 | INFO     | nwpc_workflow_log_tool.presenter.time_point_presenter:present:61 - [2020-03-16] 0 days 06:18:29

    Mean:
    0 days 06:03:17.571428

    Trim Mean (0.25):
    0 days 06:03:02
    """
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

    processor = NodeTableProcessor(
        node_path=node_path,
        target_state=target_state,
    )
    table_data = processor.process(situations)

    presenter = TimePointPresenter(
        target_node_status=node_status,
        target_state=target_state,
    )
    presenter.present(table_data)
