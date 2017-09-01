from nwpc_log_model.rdbms_model import User, Repo

from .data import repo_list
from .init_user import get_user


def create_repo(user_id, repo_name, repo_type):
    repo = Repo()
    repo.user_id = user_id
    repo.repo_name = repo_name
    repo.repo_type = repo_type
    return repo


def init_repos(session):
    repos = []
    for a_record in repo_list:
        user_name = a_record["user_name"]
        repo_name = a_record["repo_name"]
        repo_type = a_record["repo_type"]

        user = get_user(user_name, session)
        if user is None:
            continue

        repos.append(create_repo(user.user_id, repo_name, repo_type))

    for repo in repos:
        session.add(repo)
    session.commit()


def get_repo(user_id, repo_name, session):
    query = session.query(Repo).filter(Repo.user_id == user_id).filter(Repo.repo_name==repo_name)
    repo = query.first()
    return repo


def get_repo_by_name(user_name, repo_name, session):
    query = session.query(Repo).filter(Repo.user_id == Repo.user_id)\
        .filter(Repo.repo_name == repo_name) \
        .filter(User.user_name == user_name)
    repo = query.first()
    return repo


def remove_repos(session):
    repos = []
    for a_record in repo_list:
        user_name = a_record["user_name"]
        repo_name = a_record["repo_name"]

        user = get_user(user_name, session)
        if user is None:
            continue
        repo  = get_repo(user.user_id, repo_name, session)
        if repo is None:
            continue
        repos.append(repo)

    for repo in repos:
        session.delete(repo)
    session.commit()