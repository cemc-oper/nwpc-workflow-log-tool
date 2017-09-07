# coding=utf-8


from nwpc_log_model.rdbms_model.models import Record


class ModelUtil(object):
    @staticmethod
    def create_record_table(owner, repo, session):
        Record.prepare(owner, repo)
        Record.__table__.create(bind=session.get_bind(), checkfirst=True)
