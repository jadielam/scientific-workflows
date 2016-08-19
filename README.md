# Pingo
*Pingo* is a smart workflow scheduler to manage Hadoop jobs. *Pingo* takes away the burden of managing the storage
of intermediate output of computations from the humans.  Specifically, when storage space is constrained, 
*Pingo* decides what intermediate storage to keep and which one to delete depending on the optimization objective 
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
Once we understand the concept of a workflow, then we are ready to discuss how the system handles the submission of a workflow by a user.  That functionality is implemented by three different pieces of software that in conjunction produce the magic of the optimization of workflow computations.  Those three pieces are the *Action Manager*, the *Dataset Manager* and the *Workflow Manager*.  They don't share any state in memory among them, which is a stepping stone looking into a future when the system will not be a standalone server, but a cluster of servers performing the functionalities in parallel.  The core functionality of the system is carried by the *Workflow Manager*, but before jumping into it, it will be helpful to explain the functionality of the **Action Manager** first.

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
executed after action with id `id` finishes.  This is expressed by making action `1` a parent of action `2`.  Among the constraints that are imposed by the system we have the following:

1. A workflow most have at least one action.

2. No two actions can have one same `id` in a workflow definition.

3. If an action `id` is referenced somewhere in the workflow definition (they can be referenced in `startActionId`, `endActionId`, and within the array of `parentActions`), that action must be defined in the array of `actions` of the workflow.

4. The `parentActions` attribute of an `action` will define relationships among the actions that can be represented as a directed graph.  Specifically, this directed graph must be a directed acyclic graph (DAG).

5. This constraint can be derived from 4, but so that it is not overlooked, we state the rule explicitly here. The `endAction` cannot be a parent or ascendant of the `startAction`

If one of the constraints is not satisfied, the server will throw an error at workflow submission time.  A documentation of the errors thrown is provided in the REST API documentation.

Actions must have `id`, `name` and `type` attributes.  They have two optional boolean attributes: `forceComputation` and `isManaged`.  If `forceComputation` is set to `True`, it means that the action will be forced to compute its output regardless of if it already exists and is available or not. If it is set to `False`, it means that the system determines if the action will be computed or not.  The default is `False`.

If the attribute `isManaged` is set to `True`, it means that the path where the output of this action will be stored is determined and managed by the system.  If `isManaged` is set to `False`, it means that the path where the output of this action will be stored is not determined or managed by the system, and that path must be provided by the user.  The user needs to have Read/Write permissions to any path it provides, otherwise, the execution of the action will fail at the end.  The default value for `isManaged` is `True`.

Notice also how action names do not need to be unique.  An action name is just a mnemonic resource to understand what the action does.  Also depending on the action `type`, there might be other required attributes too. We currently support three kinds of actions: **Command-line actions**, **MapReduce v1.0 actions** and **MapReduce v2.0 actions**, and in the future we are planning to add support for **Spark actions** and **Sqoop actions**

TODO (Explain how an action's dataset name is determined)

### Command Line Action
TODO (Keep working here tomorrow)

### MapReduce v1.0 Action
TODO (Keep working here tomorrow)

### MapReduce v2.0 Action
TODO (Keep working here tomorrow)

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

### The Action Scraper
Every certain amount of time, the action scraper will query the database to find available actions and add them to the queue. Available actions are actions that are in the **READY** state, or actions that have been in the **PROCESSING** state for a long time.  The reason why we add actions that have been in the **PROCESSING** state for a long time is that potentially another ActionManager in another server started processing those actions, but that server died before finishing processing them.  

Before adding the action to the queue, the action scraper attempts to update the state of the action in the database to **PROCESSING**.  If the update fails because the action entity has changed in the database after it was queried by the scraper, then the scraper drops the action and does not add it to the Action Manager queue.  Otherwise, if the update is successful, the action is added to the Action Manager queue.  To illustrate how this synchronization technique is valid, consider the following example with ActionScrapers **A** and **B** and their corresponding action managers. Both scrapers **A** and **B** query the database for ready actions and both find action **a1** to be in the **READY** state.  Without loss of generality, assume that **A** is the first scraper to update the state of action **a1** to **PROCESSING**.  When **B** also attempts to update the state of action **a1**, it will realize that action **a1** has already been changed by someone else, and it will immediately drop it.

> The synchronization technique described and exemplified in the above paragraph will be used multiple times by different components of the system.  In general, that synchronization pattern can be applied in situations where multiple processes can potentially move an object **o** from state **S1** to state **S3** (in the previous example **S1** would be equivalent to our  **READY** state, and **S3** to our **SUBMITTED** state) but only one of the process should be allowed to do it.  In order to solve the problem we create an intermediate state **S2** (**PROCESSING** in our case), and we let all the processes compete to be the first to change the state of **o** to **S2**.  All the loosing processes drop the processing of object **o**, and the winning process carries on.

### The Action Submitter
The Action Manager is constantly taking new elements from the queue and passing them to the Action Submitter threads that take care of submitting the actions to Hadoop.  The decision of including in the queue actions that have been in the **PROCESSING** state for a long time makes the design of the Action Submitter more careful.  The submitter first attempts to update the state of the action to **SUBMITTED** in the database. If it succeeds, then it actually submits the action to Hadoop.  If there is an error while submitting the action, then it changes the state of the action back to **READY**, which gives that action the opportunity to be picked again by an Action Scraper at some point later on.  As an area of future improvement, a ceiling should be imposed over the number of times an action fails when submitted to the cluster, otherwise, the system will keep trying to submit the action forever.  
 
## The Workflow Manager
Now that we have explained the Action Manager, we are in a better shape to understand the workings of the workflow manager.  The workflow manager receives the workflows submitted to the system and determines which of the actions from the workflow need to actually be submitted to Hadoop for computation. Those actions are inserted into the database and can initially be in one of two states: **WAITING** or **READY**. If they are in a **READY** state, any active **Action Manager** will pick them up and submit them to the cluster for computation.  If they are in a **WAITING** state they will eventually be submitted for execution once their parents finish executing.  The process of how actions in the **WAITING** state are notified that their parents finish executing will be discussed later when we discuss the **Callback System**.

### Datasets
The workflow manager makes its decision on whether an action needs to be computed or not by exploring the state of the datasets that are the outputs of the action.  A **dataset** is another important entity in our model.  A **dataset entity** is an entry of a dataset information in the database; its **dataset file** is the physical file in the distributed file system.  A dataset entry is always linked in the database to its corresponding action definition. Dataset entities can be in one of the following states at any given time: **TO_DELETE**, **TO_STORE**, **TO_LEAF**, **STORED**, **LEAF**, **STORED_TO_DELETE**, **PROCESSING**, **DELETING** and **DELETED**.

> **TO_DELETE**: The dataset file does not exist in the file system, but once it does, its dataset entry will be transitioned to state **STORED_TO_DELETE**.

> **TO_STORE**: The dataset file does not exist in the file system, but once it does, its dataset entry state will be transitioned to **STORED**.

> **TO_LEAF**: The dataset file does not exist yet in the file system, but once it does, its dataset entry state will be transitioned to the **LEAF** state.

> **STORED**: The dataset file is stored in the filesystem and it corresponds to an intermediate action.  The dataset file will be stored in the file system until the decision algorithm determines in the future that is not optimal for the system to keep storing it anymore.

> **LEAF**: The dataset file is stored in the filesystem and it corresponds to a leaf action. Datasets of leaf actions are never removed by the system.  They can be manually removed by the users.

> **STORED_TO_DELETE**: The dataset file is stored temporarily until all other actions that have claims to it as a dependency finish computing.  Once all those actions finish computing, the dataset will be removed.

> **PROCESSING**: The dataset entry is being processed with the purpose of deleting its dataset file.  This is a synchronization state.

> **DELETING**: The dataset file is being deleted.  This is another synchronization state.

> **DELETED**: The dataset file has been deleted. 

The workflow manager processes all the actions of the submitted workflow, starting from the leaf actions in a Breadth-First-Search (BFS) manner  If by analyzing the action it determines that the action needs to be computed, it calls the `prepareForComputation` procedure on that action.

> `prepareForComputation` procedure:  The procedure first creates an action object P in the **WAITING** state and inserts it to the database.  Also, for each children C of action P that also needs to be computed, the system marks on the database that C is depending on P, so that P will need to wait for P results before being ready to be computed.  At last, the procedure adds all the parents of the action P to the queue if they have not already being added.

The *Workflow Manager* makes the determination if an action needs to be computed in the following way (pseudo code below):
```
	A = Queue.nextAction()
	if (A is not to be managed by system) OR (A's forceComputation flag is active):
		prepare A for computation
	
	else:
		if A's dataset D does not exist, or is in any of the following states (DELETED, DELETING, 
			PROCESSING, TO_DELETE, STORED_TO_DELETE:
			PREPARE ACTION A FOR COMPUTATION
		else:
			if A's dataset D is in STORED or LEAF state:
				if A is a LEAF in this workflow, but A's dataset is in STORED state:
					CHANGE ACTION A'S DATASET TO LEAF STATE
					(in this way the dataset cannot now be marked to be deleted
					by the decision algorithm)
				
				for each child C of action A:
					if C was marked for computation when it was processed:
						ADD CLAIM FROM CHILD C TO DATASET D
						
						if adding a claim to dataset D fails because D's state has changed:
							PREPARE ACTION A FOR COMPUTATION
			
			else if A's dataset D is in TO_STORE or TO_LEAF state:
				PREPARE ACTION A FOR COMPUTATION

```

I want to call the attention to three different behaviors of the algorithms described above.  First, on the `prepareForComputation` procedure, the system marks on the database that an action C is depending on an action P.  This is needed so that the Callback mechanism (which will be described later) can find which are the actions depending on action P when action P finishes computing.

Secondly, on the Workflow Manager algorithm, notice how there is a command described as *ADD CLAIM FROM CHILD C TO DATASET D*.  What this does is to add a claim from child C to the dataset entity D in the database, so that the Dataset Deletor system (to be described later) do not delete a dataset D while there is an action that depends on it that has not been computed yet.

Thirdly, for the sake of correctness of the overall state of the system, we have introduced an inefficiency in the Workflow Manager's algorithm.  Notice that if a dataset D is in **TO_STORE** or **TO_LEAF** state, we still prepare action A for computation.  A dataset D is in **TO_STORE** or **TO_LEAF** state if its corresponding action is currently computing given dataset.  This means that some othe workflow submitted to the system is currently computing dataset D. To make the system more efficient, instead of asking the system to recompute action A, we could make all the children actions of A to depend on A' (the sibling action of A from another workflow), and add a claim from the child actions of A to dataset D.  The problem with this approach is that both action A' and dataset D could be having their states changed to something contrary to the current situation at the same time we are planning to change the state of the childrens of action A with outdated information on the states of A' and D.  Trying to handle that situation would mean that we need to introduce more complex synchronization mechanisms across multiple components of the system.  For now we think that the benefits of simplicity will outweight the performance gains of trying to improve a situation that we consider will happen rarely.


## The Callback System
Once an action is submitted, three callbacks are provided to the Hadoop cluster so that it can notify back to the **Pingo** system of any relevant event regarding the execution of the action by the cluster.  All callbacks are designed in such a way that the state of the action is always the same after multiple calls to the same callback.
 
### The Success Callback
The first thing the success callback does is to change to **READY** the state of any child actions of the currently finished action that are not waiting for any other parent action to finish.  It also changes the state of the currently finished action to **FINISHED**.  TODO (Keep working here tomorrow)

### The Action-Failed Callback
TODO (Keep working here tomorrow)

### The Action-Killed Callback
TODO (Keep working here tomorrow)

## The Dataset Manager
TODO (Keep working here tomorrow)

## The Optimization Engine
TODO (Not yet implemented)
