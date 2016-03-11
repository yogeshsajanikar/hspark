module Spark

where

  import Control.Distributed.Process

  -- | Context for creating spark workflow.
  -- Defines the context for processing RDD tasks. The context stores
  -- list of peers (slaves) where the tasks can be run. The peers, in
  -- the context of Distributed.Process are nodes on which tasks can
  -- run. 
  data Context = Distributed { slaveNodes :: [NodeId] }
  

  -- | Creates the context.
  -- Note that there can only one context in the given cluster. This
  -- is not enforced yet, and creationg more than one context is not
  -- tested either.
  createContext :: IO Context
  createContext = undefined


  -- | Creates the worker
  -- Every slave should create the worker
  createWorker :: IO ()
  createWorker = undefined
