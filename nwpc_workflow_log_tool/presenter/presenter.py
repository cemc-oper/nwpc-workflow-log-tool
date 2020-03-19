import typing
from nwpc_workflow_log_tool.situation.situation_record import SituationRecord


class Presenter(object):
    def __init__(self):
        pass

    def present(self, situations: typing.Iterable[SituationRecord]):
        raise NotImplemented("Presenter is not implemented")
