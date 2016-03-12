{-# LANGUAGE GADTs #-}
{-# LANGUAGE ExistentialQuantification #-}
-- |
-- Module : Spark.RDD
--
-- RDD is represented by indexed partitions. Each partition can be
-- processed parallelly. RDD supports transformations, and grouping
-- operations. Each of these transformations produces another RDD,
-- which can be collected lazily.
--
module Spark.RDD

where

import Spark
import Control.Distributed.Process
import Control.Distributed.Process.Serializable
import qualified Data.Map as M
import Data.Typeable

-- | Partioined data, indexed by integers
newtype Partitions a = Partitions { _partitions :: M.Map Int [a] }
    deriving Show

-- | RDD aka Resilient Distributed Data
-- RDD can be created from the data itself, or it can be created as an
-- unary function map between source data and desired data. It can
-- also be created as a pair
data RDD a = RDD (Partitions a)
           | forall b . MapRDD (Closure (b -> a)) (RDD b)
           | forall b . MapRDDIO (Closure (b -> IO a)) (RDD b)

-- | Create an empty RDD
emptyRDD :: RDD a
emptyRDD = RDD $ Partitions $ M.empty

-- | Create RDD from the data itself
createRDD :: Serializable a => Context -> Int -> [a] -> RDD a
createRDD sc n xs = undefined

-- | Create map RDD from a function closure and base RDD
mapRDD :: (Serializable a, Serializable b) => Context -> Closure (a -> b) -> RDD a -> RDD b
mapRDD sc action rddA = MapRDD action rddA

-- | Map an IO action for transforming RDD.
-- This is required when distributing work across nodes to take in the
-- input data.
mapRDDIO ::
    (Serializable a, Serializable b)
    => Context
    -> Closure (a -> IO b)
    -> RDD a
    -> RDD b
mapRDDIO sc action rddA = MapRDDIO action rddA
       
-- | Process RDD and collect the data
-- This is where all the processing is initiated. The RDD is reduced
-- to number of stages. Typically these stages are 'mapping' and
-- 'reducing'. They are characterized by the fact that mapping can be
-- pipelined, whereas reduce stage typically would need a shuffle in
-- between. 
collect :: Serializable a => Context -> RDD a -> IO [a]
collect sc rdd = undefined



reduce :: RDD a -> RDD a
reduce rdda = case rdda of
                RDD ps -> RDD ps
                MapRDDIO act rddd -> reduceMapIO act (reduce rddd)
                MapRDD act rddd -> undefined

reduceMapIO :: Closure (a -> IO b) -> RDD a -> RDD b
reduceMapIO act rdda = case rdda of
                         RDD ps -> MapRDDIO act rdda
                         (MapRDD actd rddd) -> undefined


