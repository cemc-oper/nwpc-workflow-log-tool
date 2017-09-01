#!/usr/bin/env python
# coding=utf-8
"""
Alembic 中使用的 Model，models.py 中仅定义通用的记录表 Record，只对应某个 SMS Server，在使用时通过修改 __tablename__ 属性，
改变对象的映射关系，达到用一个类对应多个表的目的。
但使用 Alembic 创建表时，需要列出所有的表，所以在本模块中增加各个 SMS Server 对应的 Record 表。

后续需要修改多个相同结构表的实现方式，比如：
1. 在需要时动态生成
2. 在 Alembic 中使用单一类表示所有结构相同的表
"""

from nwpc_log_model.rdbms_model.models import Model, RecordBase


class RecordNwpXpNwpcOp(RecordBase, Model):
    __tablename__ = "record.nwp_xp.nwpc_op"

    def __init__(self):
        pass


class RecordNwpXpNwpcQu(RecordBase, Model):
    __tablename__ = "record.nwp_xp.nwpc_qu"

    def __init__(self):
        pass


class RecordNwpXpEpsNwpcQu(RecordBase, Model):
    __tablename__ = "record.nwp_xp.eps_nwpc_qu"

    def __init__(self):
        pass


class RecordNwpXPNwpcPd(RecordBase, Model):
    __tablename__ = "record.nwp_xp.nwpc_pd"

    def __init__(self):
        pass


class RecordNwpPosNwpcSp(RecordBase, Model):
    __tablename__ = "record.nwp_pos.nwpc_sp"

    def __init__(self):
        pass


class RecordNwpVfyNwpcVfy(RecordBase, Model):
    __tablename__ = "record.nwp_vfy.nwpc_vfy"

    def __init__(self):
        pass
