from nwpc_workflow_log_tool.cli.node import node_cli


import click


@click.group(name="nwpc-workflow-log-tool")
def cli():
    pass


def main():
    cli.add_command(node_cli, name="node")
    cli()


if __name__ == "__main__":
    main()
