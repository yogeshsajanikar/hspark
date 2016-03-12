{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE DeriveGeneric #-}
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

import Spark.Context
import Control.Distributed.Process
import Control.Distributed.Process.Serializable
import qualified Data.Map as M
import Data.Typeable
import Data.Binary
import GHC.Generics
import Control.Monad

-- | Partioined data, indexed by integers
newtype Partitions a = Partitions { _dataMaps :: M.Map Int [a] }
    deriving (Show, Generic)

             
instance Binary a => Binary (Partitions a)


-- Due to existential types, we will not be able to serialize RDDs,
-- however, we can serialize closures, and results of other
-- RDDs. These intermediate results need to be serialized.
                 
-- | RDD aka Resilient Distributed Data
-- RDD can be created from the data itself, or it can be created as an
-- unary function map between source data and desired data. It can
-- also be created as a pair
data RDD a = RDD (Partitions a)
           | forall b . Serializable b => MapRDD (Closure (b -> a)) (RDD b)
           | forall b . Serializable b => MapRDDIO (Closure (b -> IO a)) (RDD b)
             deriving (Typeable)

-- | Create an empty RDD
emptyRDD :: Serializable a => RDD a
emptyRDD = RDD $ Partitions $ M.empty

-- | Create RDD from the data itself
createRDD :: Serializable a => Context -> Int -> [a] -> RDD a
createRDD sc n xs = RDD $ Partitions $ partitions
    where
      partitions = M.fromList ps
      ps = zip [0..] $ splits n xs

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
       
-- | Split the data into number of partitions
-- Ensure that number of partitions are limited to maximum number of
-- elements in the list. Also that an empty list is partitioned into
-- an empty list
splits :: Int -> [a] -> [[a]]
splits n _ | n <= 0 = error "Splits must be non-negative"
splits _ [] = []
splits n xs = splits' ms xs []
    where
      l = length xs
      ms = case l `divMod` n of
             (0,rm) -> take rm $ repeat 1
             (m,0)  -> take n  $ repeat m
             (m,r)  -> take r (repeat (m+1)) ++ take (n-r) (repeat m)
                             
      splits' :: [Int] -> [a] -> [[a]] -> [[a]]
      splits' _ [] rs = reverse rs
      splits' [] _ rs = reverse rs
      splits' (m:ms) xs rs = splits' ms qs (ps:rs)
          where (ps,qs) = splitAt m xs
