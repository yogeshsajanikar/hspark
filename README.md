# Haskell Spark

This is an attempt to have functionality similar to
[Apache Spark](http://spark.apache.org/) using
[Cloud Haskell](http://haskell-distributed.github.io/). 

**Apache Spark** implements a generic cluster computing framework that
can work with multiple types of backends, such as file system,
in-memory, hadoop, mesos etc. 

Intention of this project to mimic the *Spark* computation model,
using [Cloud Haskell](http://haskell-distributed.github.io/)

## Dissecting Apache RDDs
**Spark** implements **RDD** *(Resilient Distributed Data)*. This is
resilient because it can recreate itself in case of an excepton. It
does so by remembering the lineage, and recreating itself from the
lineage, thus making it possible to recover a lost node. 

The RDD is *distributed* because it is typically distributed over the
participating nodes (called *partitions*). The distribution can by
just partitioning, or by grouping the data using hash or range
partitions. 

The RDD is lazily calculated. It is possible to create RDD by mapping
over base RDD. This way map-reduce process can be constructed using
various mapping or grouping functions. *Apache Spark* provides many
RDD types which can used to stack up RDDs.

When computed, *Spark* creates DAG of dependency RDDs and partitions,
and schedules tasks on each partition. It also tries to pipeline the
mapping operations on same node wherever possible.

### Closure Serialization
RDDs in *Spark* are serializable, in addition to this, *Spark*
implementation also serializes the *closure* of the RDDs (and
functions there within) to slave nodes, and can execute there. 

## hspark 
In hspark, an attempt is made to _mimic_ computation model of *Spark*
using *cloud haskell*. ([distributed-process](https://hackage.haskell.org/package/distributed-process-0.6.1)).

## Scope
    * Creation of simple DSL for creating map-reduce operations
    * Porting computation model over a cluster
    
The aspects which are not considered here:
    * Resiliency - The resiliency of the data is not
      considered. However, since there is a inherent dependency
      between RDDs, it should be possible to have this ability.
    * Error Handling - Current implementation depicts *happy
      scenario*, and exceptions may not be proliferated
      properly. 

## Static Pointer 
With GHC 7.10.x, GHC has **StaticPointers* extension that allows
fingerprinting the closed expression (without free variable). The
closure can be constructed around static pointers. The closure then
can be transferred over the wire.

Note that the static pointers can take care of monomorphic
functions. By using **Dict Trick**, qualified types can also be
serialized. Polymorphic types are not handled by *StaticPointers*.

*distributed-static* uses Rank1Types, and applies to *Dynamic*. This
 enables, type checking polymorphic types equality while constructing
 closures. 
 
 Since polymorphic types are not covered by **StaticPointers**
 extension, *distributed-process* constrcts "remote table* that
 contains static *labels* for various functions (including static
 pointers). This table is shared over network to help looking up the
 functions. 
 
 *hspark* leverages this closure to spawn processes over nodes. 
 
## Context
Context stores the information about slave nodes and master
node. Context also stores other information such as remote table.

## RDD Type class
Each RDD in *hspark* implements a type class called *RDD*. This type
class enables **flow** of RDD in the computation. 

RDD can be independent (such as *SeedRDD*) which is typically
starting point of the computation. Or it can be constructed from
existing RDD (dependent RDD). 

## Process as a computation and *storage*
Each RDD is further divided into *blocks*. Each block is numbered, and
actually is a process. The process here depicts an _actor_ that can do
following things:
  * **Computation** - Carrying out a unit work by applying function or 
    transformation over a data set. 
  * **Fetching** - Fetching the data from parent RDD partition (also a
    process). 
  * **Delivery** - Holding up the data till it is asked for. 
  
The *block process* exits when the data is read. In a way, the
**hspark** treats process as **distributed MVar** with input and
output channels set (and exception that it might exit after delivery
of the data to child process. Also note that during reduction step,
this may not happen).

## RDD Computation
Each RDD starts by triggering of "base" RDD. This usually returns
asynchronously, and gives access to blocks. Each block is a process. 

For each such process, the current RDD, may create another dependent
process, and would wait for parent process to deliver the data. Upon
delivery it starts working up *computation* that is is supposed to
do. 

After completion of the computation, the *process* holds the data
until asked. 

### Work Distribution
The work distribution is done by dividing the data into multiple
blocks. Each block is assgined (pushed) to a node. A node can host
multiple *processes*. 

The mapping operations are carried on the same node where parent
operation is performed. *This is currently fixed*, and should change
in the future to help *resliency*. 

### Reduction and Shuffle
Reduction step necessiates shuffling. Currently shuffling is handled
in two stages.

In the first stage, the data is reduced locally using the combining
functions. 

Second stage involves grouping based on partitioning function provided
by user (In future, this might be based on inherent property of input
data, such as range etc.). Partitioning divides the data into
independent (keys are localized), and further reduction is done. 

Since second stage (actually partitioning function) ensures
partitioning in such a way that across the group computation is
eliminated. *hspark* does not check the validity of partitioning
function. 

## Current Implementation

Current *hspark* implements following types of RDDs.
  * SeedRDD - For seeding the data.
  * MapRDD - For mapping the data.
  * MaoIORDD - For mapping an IO operation
  * ReduceRDD - For combining key value pair. 
  
### Tests
The simple tests for each RDD are in test folder. 
  
### Immediate enhancement
    * Adding *FilterRDD*
    * Adding range based partitioning
    * Error handling and linking processes so that their lifespan can
      be controlled. 
      
## References
* Apache Spark - Original Research Paper from Berkley University
  https://www.cs.berkeley.edu/~matei/papers/2012/nsdi_spark.pdf
* Mapreduce commentry by Ralf Lammel
  http://userpages.uni-koblenz.de/~laemmel/MapReduce/paper.pdf
* Distributed Process (Hackage Documentation)
  https://hackage.haskell.org/package/distributed-process-0.6.1
* Cloud Haskell and Tutorials
  http://haskell-distributed.github.io/

/Note: This work is done as a part of CS240H coursework at stanford/

