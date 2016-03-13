module Spark.Context where

  import Control.Distributed.Process
  import Control.Distributed.Static

  data Strategy = Pure
                | Distributed { slaveNodes :: [NodeId] }

  -- | Context for creating spark workflow.
  -- Defines the context for processing RDD tasks. The context stores
  -- list of peers (slaves) where the tasks can be run. The peers, in
  -- the context of Distributed.Process are nodes on which tasks can
  -- run. 
  data Context = Context { _lookupTable :: RemoteTable
                         , _strategy :: Strategy
                         }
  


  -- | Creates context from slave nodes
  createContextFrom :: [NodeId] -> IO Context
  createContextFrom = return . Context initRemoteTable . Distributed
                 
  -- | Creates the context.
  -- Note that there can only one context in the given cluster. This
  -- is not enforced yet, and creationg more than one context is not
  -- tested either.
  createContext :: IO Context
  createContext = undefined

  -- | Create pure context
  -- The RDD will run as a normal haskell program.
  defaultContext :: IO Context
  defaultContext = return $ Context initRemoteTable Pure
    
