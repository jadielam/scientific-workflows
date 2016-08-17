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

## The workflow definition language