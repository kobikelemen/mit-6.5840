# mit-6.5840
My coursework for mit-6.5840 distributed systems: [https://pdos.csail.mit.edu/6.824/index.html].

## Project Overview 
A key value service is a simple database that stores data (values) with an associated key. The goal of this project is to build a key value service on a distributed system with three key features: fault tolerance, linearizability, and sharding the data.

## Fault Tolerance [Done]
A major problem faced when building distributed systems is that each computer must remain in sync with eachother even when individual computers fail or the network fails. To achieve this, I implemented the RAFT Protocol [https://pdos.csail.mit.edu/6.824/papers/raft-extended.pdf] which solves this problem by electing a leader copmuter which coordinates the computers, and is responsible for replicating its state machine on all peers. This code can be found in the `src/main/raft` directory.

## Key Value Service [Done]
Using the fault tolerance provided by my RAFT implementation, I built a replicated key-value store. I expose two operations; put() and get(). Moreover, by using RAFT, operations on the key-value store are linearizable and the service is fault tolerant. 

## Sharded Key Value Service [Done] 
I improved the key-value service by sharding the keys into groups that can be run in parallel. Each shard consists of a subset of the total keys stored in the key-value store (e.g. keys starting with "A"-"D" could be in one shard). Moreover the machines in the cluster are split into replication groups, where each group forms a a fault tolerant Raft group that stores a few of the shards. This means that shards in different groups can be queried in parallel, increasing performance. There is also a fault tolerant coordinator, which manages group configerations (such as which shards map to which groups). This design was inspired by systems such as Spanner [https://static.googleusercontent.com/media/research.google.com/en//archive/spanner-osdi2012.pdf].
