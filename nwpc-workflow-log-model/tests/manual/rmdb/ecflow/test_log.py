# coding: utf-8
import click
from nwpc_workflow_log_model.rmdb.ecflow.record import EcflowRecordBase


@click.command()
@click.option('-f', 'log_file')
def cli(log_file):
    with open(log_file) as f:
        for line in f:
            record = EcflowRecordBase.parse(line.strip())


if __name__ == "__main__":
    cli()
