# coding: utf-8
import datetime

from mongoengine import Document, StringField, DateTimeField, GenericEmbeddedDocumentField


class Blob(Document):
    owner = StringField(required=True)
    repo = StringField(required=True)
    timestamp = DateTimeField(default=datetime.datetime.utcnow)
    data = GenericEmbeddedDocumentField()

    meta = {
        'allow_inheritance': True,
        'abstract': True
    }
