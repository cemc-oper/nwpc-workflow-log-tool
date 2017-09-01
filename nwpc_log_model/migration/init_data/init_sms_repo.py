from nwpc_log_model.rdbms_model import User, Repo, SmsRepo

from .data import repo_list
from .init_repo import get_repo_by_name


def create_sms_repo(user_id, repo_id, repo_name):
    repo = SmsRepo()
    repo.user_id = user_id
    repo.repo_id = repo_id
    repo.repo_name = repo_name
    return repo


def init_sms_repos(session):
    repos = []
    for a_record in repo_list:
        user_name = a_record["user_name"]
        repo_name = a_record["repo_name"]
        repo_type = a_record["repo_type"]

        if repo_type is not 'sms':
            continue

        repo = get_repo_by_name(user_name, repo_name, session)
        if repo is None:
            continue

        repos.append(create_sms_repo(repo.user_id, repo.repo_id, repo_name))

    for repo in repos:
        session.add(repo)
    session.commit()


def get_sms_repo(user_name, repo_name, session):
    query = session.query(Repo) \
        .filter(Repo.user_id == User.user_id) \
        .filter(Repo.repo_name == repo_name) \
        .filter(User.user_name == user_name)
    repo = query.first()
    return repo


def remove_sms_repos(session):
    repos = []
    for a_record in repo_list:
        user_name = a_record["user_name"]
        repo_name = a_record["repo_name"]
        repo_type = a_record["repo_type"]

        if repo_type is not 'sms':
            continue

        repo  = get_sms_repo(user_name, repo_name, session)
        if repo is None:
            continue
        repos.append(repo)

    for repo in repos:
        session.delete(repo)
    session.commit()