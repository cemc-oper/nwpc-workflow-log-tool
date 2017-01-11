# An NWPC work flow model for SMS

A general model for NWPC's SMS projects, including tree, node, node status, visitor functions and so on.

## node

A node is a task or a container which contains other nodes.

## node status

Each node has a status at certain time to indicate what is going on. For example:

* an `active` task means there is a task running.
* an `aborted` task means that some task has an error.

## bunch

A bunch represents a tree of nodes. 
In our operation system, one bunch is managed by one server program.

## visitor

This model also provides some access traversing functions which ars useful for dealing with bunches.
