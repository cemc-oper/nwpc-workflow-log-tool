# coding: utf-8

from mongoengine import StringField, DateTimeField, DictField, EmbeddedDocument, EmbeddedDocumentField


from .blob import Blob


class NodeStatusBlobData(EmbeddedDocument):
    node = StringField()
    date = DateTimeField()
    type = StringField(choices=['suite', 'family', 'task'])
    time_point = DictField()
    time_period = DictField()


class NodeStatusBlob(Blob):
    data = EmbeddedDocumentField(NodeStatusBlobData)

    meta = {
        'collection': 'node_status'
    }
