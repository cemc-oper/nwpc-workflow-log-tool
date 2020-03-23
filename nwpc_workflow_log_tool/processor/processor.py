import typing
from nwpc_workflow_log_tool.situation import SituationRecord


class Processor(object):
    def __init__(self):
        pass

    def process(self, situations: typing.Iterable[SituationRecord]):
        pass
