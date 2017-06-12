from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker

from nwpc_log_model.rdbms_model import User, Repo, RepoVersion, RecordBase

engine = create_engine(
    'mysql+mysqlconnector://windroc:shenyang@10.28.32.175/system-time-line',
    # echo=True
)
Session = sessionmaker(bind=engine)
session = Session()


def check_repo(user, repo):
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


def add_new_repo_version(repo, head_line):
    version = RepoVersion()
    version.repo_id = repo.repo_id
    r = RecordBase()
    r.parse(head_line)
    version_date = r.record_date.strftime("%Y%m%d")
    query = session.query(RepoVersion).filter(RepoVersion.repo_id == repo.repo_id) \
        .filter(func.substr(RepoVersion.version_id, 0, 8)).order_by(RepoVersion.version_id.desc())

    latest_version = query.first()
    if latest_version is None:
        version_id = int(version_date)*100
        sub_version = 0
    else:
        version_id = latest_version.version_id + 1
        sub_version = latest_version.version_id % 100 + 1
    version.version_id = version_id
    version.version_name = "version." + r.record_date.strftime("%Y-%m-%d") + "." + str(sub_version)
    version.head_line = head_line
    session.add(version)
    session.commit()
    return version


def get_version(user_name, repo_name, head_line):
    c = check_repo(user_name, repo_name)
    if c is None:
        return None
    (user, repo) = c

    version = session.query(RepoVersion) \
        .filter(RepoVersion.repo_id == repo.repo_id) \
        .filter(RepoVersion.head_line == head_line).first()
    if version is None:
        print("version not found, add a new version")
        version = add_new_repo_version(repo, head_line)
    else:
        print("version found, use existed version")
    return version
