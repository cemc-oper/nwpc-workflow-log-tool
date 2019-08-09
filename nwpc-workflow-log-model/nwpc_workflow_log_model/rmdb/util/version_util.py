# coding: utf-8
from sqlalchemy import func

from nwpc_workflow_log_model.rmdb.base.owner import Owner
from nwpc_workflow_log_model.rmdb.base.repo import Repo, RepoVersion


class VersionUtil(object):
    @classmethod
    def check_repo(cls, session, owner, repo):
        result = session.query(Owner, Repo).filter(Owner.owner_name == owner) \
            .filter(Repo.repo_name == repo) \
            .filter(Owner.owner_id == Repo.owner_id) \
            .first()

        if result is None:
            return None

        owner, repo = result

        if owner is None or repo is None:
            return None
        return owner, repo

    @classmethod
    def add_new_repo_version(cls, session, repo, location, head_line, record_class):
        version = RepoVersion()
        version.repo_id = repo.repo_id
        r = record_class()
        r.parse(head_line)
        version_date = r.date.strftime("%Y%m%d")
        query = session.query(RepoVersion).filter(RepoVersion.repo_id == repo.repo_id) \
            .filter(func.substr(RepoVersion.version_id, 1, 8) == version_date).order_by(RepoVersion.version_id.desc())

        latest_version = query.first()
        if latest_version is None:
            version_id = int(version_date)*100 + 1
            sub_version = 1
        else:
            last_version_id = int(latest_version.version_id)
            version_id = last_version_id + 1
            sub_version = last_version_id % 100 + 1
        version.version_id = str(version_id)
        version.version_name = "version." + r.date.strftime("%Y-%m-%d") + "." + str(sub_version)
        version.version_location = location
        version.head_line = head_line
        session.add(version)
        session.commit()
        return version

    @classmethod
    def get_version(cls, session, owner, repo, location, head_line, record_class):
        c = VersionUtil.check_repo(session, owner, repo)
        if c is None:
            return None
        (user, repo) = c

        version = session.query(RepoVersion) \
            .filter(RepoVersion.repo_id == repo.repo_id) \
            .filter(RepoVersion.head_line == head_line).first()
        if version is None:
            print("version not found, add a new version")
            version = VersionUtil.add_new_repo_version(session, repo, location, head_line, record_class)
        else:
            print("version found, use existed version")
        return version
