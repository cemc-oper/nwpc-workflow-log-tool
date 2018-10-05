# coding: utf-8

from mongoengine import DateTimeField, DictField, EmbeddedDocument, EmbeddedDocumentField


from .blob import Blob


class NodeTreeBlobData(EmbeddedDocument):
    date = DateTimeField()
    tree = DictField()


class NodeTreeBlob(Blob):
    data = EmbeddedDocumentField(NodeTreeBlobData)

    meta = {
        'collection': 'node_tree'
    }
