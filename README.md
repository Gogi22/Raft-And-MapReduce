# Raft And MapReduce

This Project is an MIT Distributed Systems Course Lab that covers both Mapreduce and Raft distributed consensus algorithm.

Raft is a distributed consensus algorithm that uses leader elections to solve the problem of getting multiple servers to agree on a shared state, even in the cases of failures. Compared to MapReduce it was considerably more complex. I implemented Raft as a Go object type with associated methods, meant to be used as a module in a larger service. A set of Raft instances talk to each other with RPC to maintain replicated logs. This Raft interface supports an indefinite sequence of numbered commands, also called log entries. The entries are numbered with _index numbers. The log entry with a given index will eventually be committed. At that point, Raft sends the log entry to the larger service for it to execute. 
> Code for Raft Implementation is in the src/raft folder.

As for MapReduce Lab, it is a programming model or a pattern that is used to access big data stored in systems like GFS or Hadoop. It Facilitates concurrent processing by splitting big data into smaller chunks and then processing them in parallel. In this Lab, there was a basic layout on which I implemented a distributed MapReduce, consisting of two programs, the master and the worker. The workers talk to the master via RPC. Each worker process asks the master for a task, reads the task's input from one or more files, executes the task, and writes the task's output to one or more files. The master notices if a worker hasn't completed its task in a reasonable amount of time and gives the same task to a different worker. 
> Code for MapReduce Implementation is in the src/mr folder.
