{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE GADTs #-}
module Spark.DSL where

import Control.Distributed.Process



data RDD :: * -> * where
  InitRDD :: Traversable t => Closure (t a) -> Int -> RDD a
  MapRDD :: Closure (a -> b) -> RDD a -> RDD b
  FilterRDD :: Closure (a -> Bool) -> RDD a -> RDD a
  ReduceRDD :: Ord k => Closure (k -> v -> v -> u) -> RDD (k, v) -> RDD (k, u)
  

-- | A IORef like variable held within a process.
-- The process returns with IORef like construct.
  
data NVarContents 
