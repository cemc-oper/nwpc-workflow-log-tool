# coding: utf-8
from sqlalchemy import Column, Integer, String, Text
from nwpc_workflow_log_model.rmdb.base.model import Model
from nwpc_workflow_log_model.rmdb.base.owner import Owner
from nwpc_workflow_log_model.rmdb.base.repo import Repo
from .record import EcflowRecord


class EcflowRepo(Model):
    __tablename__ = "ecflow_repo"

    # 仅该表中使用，其它表中使用 repo_id
    repo_id = Column(Integer, primary_key=True, autoincrement=False)
    owner_id = Column(Integer, nullable=False)
    repo_name = Column(String(45))
    repo_location = Column(String(200))
    current_version_id = Column(String(20))
    repo_description = Column(Text())

    def __init__(self):
        pass

    def update_from_dict(self, repo_dict):
        if self.repo_id != repo_dict['repo_id']:
            return False
        # 只更新一部分内容，后面需要更改
        self.repo_location = repo_dict['repo_location']
        self.current_version_id = repo_dict['current_version_id']
        self.repo_description = repo_dict['repo_description']
        return True

    @classmethod
    def create_repo(cls, owner, repo, session):
        owner_object = Owner.create_owner(owner, session)

        repo_object = Repo()
        repo_object.repo_name = repo
        repo_object.repo_type = 'sms'
        repo_object.owner_id = owner_object.owner_id

        result = session.query(Repo).filter(Repo.repo_name == repo) \
            .filter(owner_object.owner_id == Repo.owner_id) \
            .first()
        if not result:
            session.add(repo_object)
            session.commit()
        else:
            repo_object = result

        result = session.query(EcflowRepo).filter(EcflowRepo.repo_id == repo_object.repo_id).first()
        if not result:
            ecflow_repo_object = EcflowRepo()
            ecflow_repo_object.repo_name = repo
            ecflow_repo_object.repo_id = repo_object.repo_id
            ecflow_repo_object.owner_id = repo_object.owner_id
            session.add(ecflow_repo_object)
            session.commit()

        EcflowRecord.prepare(owner, repo)
        if not EcflowRecord.__table__.exists(bind=session.get_bind()):
            EcflowRecord.create_record_table(owner, repo, session)