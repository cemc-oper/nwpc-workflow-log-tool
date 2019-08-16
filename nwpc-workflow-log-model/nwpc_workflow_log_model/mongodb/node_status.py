# coding: utf-8
import logging
import datetime
from mongoengine import (
    StringField,
    DateTimeField,
    DictField,
    EmbeddedDocument,
    EmbeddedDocumentField,
)

from .blob import Blob


class NodeStatusBlobData(EmbeddedDocument):
    node = StringField()
    date = DateTimeField()
    type = StringField(choices=["suite", "family", "task"])
    time_point = DictField()
    time_period = DictField()


class NodeStatusBlob(Blob):
    data = EmbeddedDocumentField(NodeStatusBlobData)

    meta = {"collection": "node_status"}


def save_node_status(
    owner: str,
    repo: str,
    start_date: datetime.date,
    end_date: datetime.date,
    date_node_status_list: dict,
    update_type="insert",
):
    total_count = len(date_node_status_list)
    logging.info(
        "Add {total_count} node status to mongodb...".format(total_count=total_count)
    )

    if update_type == "insert":
        # delete previous data
        NodeStatusBlob.objects(
            owner=owner, repo=repo, data__date__gte=start_date, data__date__lte=end_date
        ).delete()

    cur_count = 0
    cur_percent = 0

    # status_list_to_be_inserted = []

    for status in date_node_status_list:
        cur_count += 1
        percent = int(cur_count * 100.0 / total_count)
        if percent > cur_percent:
            print("[{percent}%]".format(percent=percent))
            cur_percent = percent
        if status is None:
            continue
        a_node_status_key, a_node_status = status
        if a_node_status["type"] == "family" or a_node_status["type"] == "task":
            if update_type == "upsert":
                NodeStatusBlob.objects(
                    owner=owner,
                    repo=repo,
                    data__date=a_node_status["date"],
                    data__node=a_node_status["node"],
                ).update_one(
                    set__type=a_node_status["type"],
                    set__time_point=a_node_status["time_point"],
                    set__time_period=a_node_status["time_period"],
                    upsert=True,
                )
            else:
                node_status = NodeStatusBlob(
                    owner=owner,
                    repo=repo,
                    data=NodeStatusBlobData(
                        node=a_node_status["node"],
                        date=a_node_status["date"],
                        type=a_node_status["type"],
                        time_point=a_node_status["time_point"],
                        time_period=a_node_status["time_period"],
                    ),
                )
                node_status.save()
                # status_list_to_be_inserted.append(node_status)
                # if len(status_list_to_be_inserted) >= MAX_INSERT_COUNT:
                #     print("Saving documents into mongodb...")
                #     NodeStatusBlob.objects.insert(status_list_to_be_inserted)
                #     status_list_to_be_inserted = []

        # if len(status_list_to_be_inserted) > 0:
        #     NodeStatusBlob.objects.insert(status_list_to_be_inserted)
