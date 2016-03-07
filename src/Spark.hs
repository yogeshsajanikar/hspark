module Spark

where

-- | Context for creating spark workflow.
data Context = Context { numPartitions :: Int }
