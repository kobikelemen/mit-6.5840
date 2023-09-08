# mit-6.5840
My attempt at doing the coursework for mit-6.5840 distributed systems

I am completing the following course focused on distributed systems: https://pdos.csail.mit.edu/6.824/index.html 

This project aims to build a fault tolerant, parallel, key value service.

## What does this mean?
A key value service is a program that takes in data and for each element in the data - the 'key' - it does some computation to get the associated value. An example is counting the occurences of each word in some text. In this case, the data is the text, the keys are the words, and the values are the number of occurences of that word.
The goal of this project is to run this key value service on a distributed system of many computers to increase performance. For this to produce valid results, the distributed system must be fault tolerant. And the computation must be split in a way that allows it to be performed on many computers in parallel.

## Key Value Service
The key value service I implement is Map Reduce [https://pdos.csail.mit.edu/6.824/papers/mapreduce.pdf]. This takes in three things: the data, a 'mapping' function, and a 'reduce' function. The data is first split into chunks so that each computer processes a subset of it. The mapping function is then run on each chunk to produce the key value pairs and saved in intermediate files. The reduce function is then run to collate the pairs into an overall result. Please refer to the paper for a more detailed explanation and examples.

## Fault Tolerance
A major problem faced when building distributed systems is that each computer must remain in sync with eachother even when individual computers fail or the network fails. To achieve this, I implemented the RAFT Protocol [https://pdos.csail.mit.edu/6.824/papers/raft-extended.pdf] which solves this problem by electing a leader copmuter which coordinates the computers, and is responsible for replicating its state machine on all peers. This code can be found in the `src/main/raft` directory.

## Sharded Key Value Service [In progress] 
The goal of this is to run the key value service on the fault tolerant distributed system, such that the service is run in parallel across the system.





