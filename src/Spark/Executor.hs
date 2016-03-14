-- |
-- Module: Spark.Executor
--
-- Each block is defined by a block of data residing in a process. We
-- spawn the process on remote nodes, and send the data. Similarly a
-- mapping stage defines a process that fetches the data from
-- dependent process and holds it till asked by master or another
-- process.
--
module Spark.Executor where


import Spark.Context
import Spark.RDD
import Control.Distributed.Process
import Control.Distributed.Process.Serializable
import Spark.Pure.Exec
    
-- | Process RDD and collect the data
-- This is where all the processing is initiated. The RDD is reduced
-- to number of stages. Typically these stages are 'mapping' and
-- 'reducing'. They are characterized by the fact that mapping can be
-- pipelined, whereas reduce stage typically would need a shuffle in
-- between. 
collect :: (RDD a b, Serializable b) => Context -> a b -> IO [b]
collect sc@(Context _ Pure) = executePure sc
collect sc@(Context _ (Distributed master nodes)) = do
  undefined


executeDistributed :: (RDD a b, Serializable b) => Context -> [NodeId] -> a b -> IO [b]
executeDistributed = undefined


