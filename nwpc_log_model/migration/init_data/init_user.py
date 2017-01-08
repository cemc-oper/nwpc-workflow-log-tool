import os
import sys
sys.path.append(os.path.dirname(os.path.abspath(__file__)) + "/../../../")
from nwpc_log_model.rdbms_model import User

from .data import user_list


def create_user(user_name):
    user = User()
    user.user_name = user_name
    return user


def initial_users(session):
    users = []
    for an_user in user_list:
        users.append(create_user(an_user["user_name"]))

    for user in users:
        session.add(user)

    session.commit()


def get_user(user_name, session):
    query = session.query(User).filter(User.user_name==user_name)
    user = query.first()
    return user


def remove_users(session):
    users = []
    for an_user in user_list:
        users.append(get_user(an_user["user_name"], session))

    for user in users:
        if user is not None:
            session.delete(user)
    session.commit()