# scientific-workflows
Apache Oozie's wrapper that manages the storage of intermediate datasets in workflow computations.

## Reason behind the project
*Apache Oozie* is a workflow scheduler system to manage Apache Hadoop jobs. Oozie
workflow jobs are Directed Acyclic Graphs (DAGs) of actions.  In general, workflows
are composed of actions that might take data as input, and that will also output
data that will be consumed by other actions as input.  The datasets produced
by actions with the sole purpose of being consumed by other actions are called
*intermediate datasets*.

Storing the intermediate datasets can potentially save time on future 
workflow computations that might have to use those datasets.  But since storage
space is limited, not all the intermediate datasets can be stored.  Determining
at any given time which datasets to save in order to optimize workflow computation
time of future workflow submissions it is a hard an interesting problem that is being actively researched.  The purpose *scientific-workflows* is to provide a 
framework that will make easy to *develop* and *evaluate* optimization/decision algorithms that attempt to solve the *storage of intermediate datasets* problem.

We have decided to design the system as a wrapper around [Apache Oozie](https://oozie.apache.org) since Oozie is a very mature and scalable system being used in production in many different settings.

# Description of the System

The system is made of three main components: 

1. A replica of the Oozie REST API.
2. An Accounting Module that keeps statistics on the submission and execution of workflows by the system.
3. An Decision Module that at any given workflow submission in time determines which new *intermediate datasets* should be stored and which stored *intermediate datasets* should be deleted in order to optimize the computation time of future workflow submissions.
4. The Execution Module. It takes care of submitting to Oozie the workflows submitted by the users for execution. It introduces into the workflow definitions some transformations that are irrelevant to the end user.
4. An Actuator Module that takes care of storing and removing datasets.

## Replica of the Oozie REST API.
