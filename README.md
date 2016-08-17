# Pingo
*Pingo* is a workflow scheduler to manage Hadoop jobs that takes away the burden of managing the storage
of intermediate output of computations from the humans.  Specifically, when storage space is constrained, 
Pingo decides what intermediate storage to keep and which one to delete depending on the optimization objective 
of the decision algorithm being used.  But Pingo is more than a test bed for optimization algorithms that attempt to
find optimal solutions to the data reuse problem under storage constraints.  It also provides the following 
functionality and contributions: 
- Pingo runs as a server (cluster of servers in future work), managing multiple users submitting workflows at the 
same time.  It has a very light memory footprint as it keeps most of its state in a database. 
- A REST API that can be used to remotely submit workflows of computation to a Hadoop cluster.  The API also
allows for the querying of the state of the computation of a submitted workflow.  Pingo keeps the state of
all running workflows in a MongoDB database.  A workflow state is in memory only during state transition periods.
- A well-thought workflow description language that is expressive enough to provide support for most workflow use cases,
as well as constrained enough as to allow the system to successfully index and capture the data provenance of datasets
by just reading the workflow definition of the computational tasks that produced them.
- A smart storage system that knows how to link together *computational task definition* and *location* of
its corresponding output data in the distributed file system. The storage system keeps an index of every dataset
ever computed by the engine, together with metadata such as size, state, and availability.
- A workflow manager that modifies the definition of workflows submitted by the users, removing redundant computations
when the datasets for those computations have been previously computed and are currently available for usage. 


## Reason behind the project
There are currently in existance workflow scheduling systems that manage the submission of computational jobs. 
Among the best known systems we find *Apache Oozie* and *Azkaban*.  In particularly, Apache Oozie
workflow jobs are Directed Acyclic Graphs (DAGs) of actions.  In general, workflows
are composed of actions that might take data as input, and that will also output
data that will be consumed by other actions as input.  The datasets produced
by actions with the *sole* purpose of being consumed by other actions are called
*intermediate datasets*.

Storing the intermediate datasets can potentially save time on future 
workflow computations that might have to use those datasets.  But since storage
space is limited, not all the intermediate datasets can be stored.  Determining
at any given time which datasets to save in order to optimize workflow computation
time of future workflow submissions it is a hard an interesting problem that is being actively researched.  The purpose *Pingo* is twofold.  Its first purpose is to provide a framework that will make easy to *research* (develop and evaluate) optimization/decision algorithms that attempt to solve the *storage of intermediate datasets* problem.
The second purpose is to be used in *production* as a robust system to manage the persistance of the data 
produced by computational jobs, being flexible enough to allow both manual and automatic management of the
lifespan of the datasets of the data pool.

# Description of the System
In the following sections we will describe how the system works. We first start by introducing the workflow definition
language.  That will help us understand what is a workflow and what are the constraints and functionalities of the system.
Once we understand what is a workflow, then we are ready to discuss how the system handles the submission of a workflow by a user.  That functionality is implemented by three different pieces of software that in conjunction produce the magic of the optimization of workflow computations.  Those three pieces are the *Action Manager*, the *Dataset Manager* and the *Workflow Manager*.  They don't share any state in memory among them, which is a stepping stone looking into a future when the system will not be a standalone server, but a cluster of servers performing the functionalities in parallel.  The *Workflow Manager* will be the last component to be explained, since it is the most complex of the three.

## The Workflow Definition Language
We have chosen the JSON format for the definition of workflows because its expressiveness is sufficient for what
we need, and it is also very pleasant to the eye.  A workflow is made of a `name`, an `startActionId`, 
an `endActionId` and a list of actions.  See the example below:

	{
		"name": "Example Workflow",
		"startActionId": 1,
		"endActionId": 2,
		"actions": [
			{
				"id": 1,
				"name": "action1-name",
				"type": "command-line",
				.
				.
				.
			},
			{
				"id": 2,
				"name": "action1-name",
				"parentActions": [
					{ 
						"id": 1,
					}
				],
				"type": "command-line",
				.
				.
				.
			}
		]
	}

The workflow definition above consists of two actions whose ids are `1` and `2` where action with id `2` must be
executed after action with id `id` finishes.  This is expressed by making action `1` a parent of action `2`. Actions must
have `id`, `name` and `type` attributes, and also depending on the action `type`, there might be other required attributes too. Among the constraints that are imposed by the system we have the following:

1. A workflow most have at least one action.

2. No two actions can have one same `id` in a workflow definition.

3. If an action `id` is referenced somewhere in the workflow definition (they can be referenced in `startActionId`, `endActionId`, and within the array of `parentActions`), that action must be defined in the array of `actions` of the workflow.

4. The `parentActions` attribute of an `action` will define relationships among the actions that can be represented as a directed graph.  Specifically, this directed graph must be a directed acyclic graph (DAG).

5. This constraint can be derived from 4, but so that it is not overlooked, we state the rule explicitly here. The `endAction` cannot be a parent or ascendant of the `startAction`

If one of the constraints is not satisfied, the server will throw an error at workflow submission time.  A documentation of the errors thrown is provided in the REST API documentation.

Notice how action names do not need to be unique.  An action name is just a mnemonic resource to understand what the action does.

Currently we support three kinds of actions: Command-line actions, MapReduce v1.0 actions and MapReduce v2.0 actions.

### Command Line Action

### MapReduce v1.0 Action

### MapReduce v2.0 Action

## The Action Manager
The Action Manager's purpose is to submit individual actions to the Hadoop cluster for computation.  In our current implementation it uses *Apache Oozie* as an intermediary, but there is no restriction in our system to do away with *Apache Oozie* in the future.  

On its current implementation, the Action Manager is ready to be distributed across different machines. That is, if there are multiple action managers running on different machines, they will not step up on each other toes, because they use the database as a mean of synchronization among them.

The ActionManager works as follows:

1. It maintains a synchronized queue Q with the actions that need to be submitted to the Hadoop cluster.  The queue is capacity bounded and supports operations that wait for the queue to become non-empty when retrieving an element, and wait for space to become available in the queue when storing an element.  All operations are thread safe.

2. The queue is filled by an ActionScraper entity that queries the database for actions that are ready to be submitted.  

3. The ActionManager takes new actions from the queue and hands them to a pool of ActionSubmitter threads that will submit the actions to Hadoop and will also update the state of those actions in the database.

### Action States
In order to support a cluster of servers working as action managers and to avoid the need to add a dependency to a distributed coordination server such as Apache Zookeper we have implemented synchronization using the database as our shared resource and defining a synchronization oriented semantic for each of the different states an action can have.

An action can be in one of the following states: **WAITING**, **READY**, **PROCESSING**, **SUBMITTED**, **RUNNING**, **FINISHED**, **FAILED**, and **KILLED**.

> **WAITING**: It means that the action has been submitted as part of a workflow and is waiting for parent actions to finish before it can be submitted to Hadoop.

> **READY**: The action is ready to be submitted to Hadoop because it either does not depend on any other action, or because all the actions on which it depends have finished their computations.

> **PROCESSING**: The ActionScraper found a READY action in the database and has placed it in the actions queue of the actions to be submitted.

> **SUBMITTED**: The action has been taken from the queue and has been submitted to Hadoop.

> **RUNNING**: Hadoop is running the computations that correspond to the action.

> **FINISHED**: Hadoop has finished executing the action successfully.

> **FAILED**: A run time error has occurred and the action did not finish executing.

> **KILLED**: The user killed the action after it started executing.

Actions can be in one of multiple states 

## The Dataset Manager 
## The Workflow Manager
The purpose of the workflow manager is to, if possible, reduce the workflow submitted by an user by identifying 
the actions that do not need to be computed because their outputs have already being computed previously and are still
present in the system.
