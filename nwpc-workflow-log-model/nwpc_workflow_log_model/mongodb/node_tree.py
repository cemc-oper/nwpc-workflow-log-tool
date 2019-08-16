# coding: utf-8
import datetime
import logging
from mongoengine import (
    DateTimeField,
    DictField,
    EmbeddedDocument,
    EmbeddedDocumentField,
)
from nwpc_workflow_model.bunch import Bunch

from .blob import Blob


class NodeTreeBlobData(EmbeddedDocument):
    date = DateTimeField()
    tree = DictField()


class NodeTreeBlob(Blob):
    data = EmbeddedDocumentField(NodeTreeBlobData)

    meta = {"collection": "node_tree"}


def save_bunch(
    owner: str, repo: str, query_date: datetime.date, bunch: Bunch, update_type="insert"
):
    """

    :param owner:
    :param repo:
    :param query_date:
    :param bunch:
    :param update_type: insert or upsert
    :return:
    """
    # saving results to mongodb
    if update_type == "insert":
        # delete previous data
        results = NodeTreeBlob.objects(
            owner=owner, repo=repo, data__date=query_date
        ).delete()

    logging.info(query_date)

    cur_query_datetime = datetime.datetime.combine(query_date, datetime.time())

    node_tree = NodeTreeBlob(
        owner=owner,
        repo=repo,
        data=NodeTreeBlobData(date=cur_query_datetime, tree=bunch.to_dict()),
    )

    logging.info(node_tree.owner, node_tree.repo, node_tree.data.date)
    if update_type == "upsert":
        NodeTreeBlob.objects(
            owner=owner, repo=repo, data__date=cur_query_datetime
        ).update_one(set__data__tree=bunch.to_dict(), upsert=True)
    else:
        node_tree.save()


def save_bunch_map(
    owner: str,
    repo: str,
    start_date: datetime.date,
    end_date: datetime.date,
    bunch_map: dict,
    update_type="insert",
):
    # saving results to mongodb
    if update_type == "insert":
        # delete previous data
        results = NodeTreeBlob.objects(
            owner=owner,
            repo=repo,
            data__date__gte=start_date - datetime.timedelta(days=1),
            data__date__lte=end_date,
        ).delete()

    total_count = len(bunch_map)
    logging.info(
        "Adding {total_count} tree status to mongodb".format(total_count=total_count)
    )
    cur_count = 0
    cur_percent = 0
    for cur_query_date in bunch_map:
        logging.info(cur_query_date)
        cur_count += 1
        percent = int(cur_count * 100.0 / total_count)
        if percent > cur_percent:
            logging.info("[{percent}%]".format(percent=percent))
            cur_percent = percent
        cur_query_datetime = datetime.datetime.combine(cur_query_date, datetime.time())
        cur_bunch = bunch_map[cur_query_date]

        node_tree = NodeTreeBlob(
            owner=owner,
            repo=repo,
            data=NodeTreeBlobData(date=cur_query_datetime, tree=cur_bunch.to_dict()),
        )

        logging.info(node_tree.owner, node_tree.repo, node_tree.data.date)
        if update_type == "upsert":
            NodeTreeBlob.objects(
                owner=owner, repo=repo, data__date=cur_query_datetime
            ).update_one(set__tree=cur_bunch.to_dict(), upsert=True)
        else:
            node_tree.save()
