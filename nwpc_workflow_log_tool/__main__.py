from nwpc_workflow_log_tool.node import cli as node_cli


import click


@click.group(name="nwpc-workflow-log-tool")
def cli():
    pass


cli.add_command(node_cli, name="node")


if __name__ == "__main__":
    cli()
