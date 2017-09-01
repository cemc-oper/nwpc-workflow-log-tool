from sqlalchemy import func

from nwpc_log_model.rdbms_model import User, Repo, RepoVersion, RecordBase


class VersionUtil(object):
    @staticmethod
    def check_repo(user, repo, session):
        result = session.query(User, Repo).filter(User.user_name == user) \
            .filter(Repo.repo_name == repo) \
            .filter(User.user_id == Repo.user_id) \
            .first()

        if result is None:
            return None

        user, repo = result

        if user is None or repo is None:
            return None
        return user, repo

    @staticmethod
    def add_new_repo_version(repo, location, head_line, session):
        version = RepoVersion()
        version.repo_id = repo.repo_id
        r = RecordBase()
        r.parse(head_line)
        version_date = r.record_date.strftime("%Y%m%d")
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
        version.version_name = "version." + r.record_date.strftime("%Y-%m-%d") + "." + str(sub_version)
        version.version_location = location
        version.head_line = head_line
        session.add(version)
        session.commit()
        return version

    @staticmethod
    def get_version(owner, repo, location, head_line, session):
        c = VersionUtil.check_repo(owner, repo, session)
        if c is None:
            return None
        (user, repo) = c

        version = session.query(RepoVersion) \
            .filter(RepoVersion.repo_id == repo.repo_id) \
            .filter(RepoVersion.head_line == head_line).first()
        if version is None:
            print("version not found, add a new version")
            version = VersionUtil.add_new_repo_version(repo, location, head_line, session)
        else:
            print("version found, use existed version")
        return version
