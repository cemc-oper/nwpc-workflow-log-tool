# coding: utf-8
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


def get_session(database_uri: str):
    engine = create_engine(database_uri)
    Session = sessionmaker(bind=engine)
    session = Session()
    return session
