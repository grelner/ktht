# Toy payments engine implementation #

The system is designed around a share-nothing thread per core architecture that allows processing transactions 
concurrently, while maintaining order correctness. The system uses no synchronization primitives and is lock-free during 
the computation phase. The solution is not necessarily optimal for the task of parsing a small csv file and outputting 
the result as csv. However, it is rather meant to demonstrate the core concepts of one way to design a high-throughput 
stream processing system that solves a problem of this type.

I chose not to use an asynchronous runtime for this project, as I feel the code in its current state more clearly
shows the threading model and core concepts. In the real world, I would most likely use an asynchronous runtime like tokio
for networking and task scheduling. A thread per core model like the one used in this project can also be used with
asynchronous runtimes by using native threads which each have a single threaded runtime.

## AI Usage ##
Some comments and tests were generated using RustRover built-in AI tools, and then proofread and usually heavily modified.
There was no AI usage in building the actual functionality.