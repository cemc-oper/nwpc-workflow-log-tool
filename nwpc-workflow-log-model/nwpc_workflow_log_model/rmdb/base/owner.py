# coding: utf-8
from sqlalchemy import Column, Integer, String
from .model import Model


class Owner(Model):
    __tablename__ = "owner"

    owner_id = Column(Integer, primary_key=True, autoincrement=True)
    owner_name = Column(String(45))

    def __init__(self):
        pass

    @classmethod
    def create_owner(cls, owner, session):
        owner_object = cls()
        owner_object.owner_name = owner
        result = session.query(cls).filter(cls.owner_name == owner) \
            .first()
        if not result:
            owner_object = session.merge(owner_object)
            session.commit()
        else:
            owner_object = result
        return owner_object
