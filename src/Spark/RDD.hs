{-# LANGUAGE ExistentialQuantification #-}
module Spark.RDD

where

import Spark
import Control.Distributed.Process


-- | RDD aka Resilient Distributed Data
-- RDD can be created from the data itself, or it can be created as an
-- unary function map between source data and desired data. It can
-- also be created as a pair
data RDD a = RDD a -- Pure RDD
           | forall b . MapRDD (Closure (b -> a)) (RDD a) 


-- | Create RDD from the data itself
createRDD :: Context -> [a] -> RDD a
createRDD = undefined

-- | Create map RDD from a function closure and base RDD
mapRDD :: Context -> Closure (a -> b) -> RDD a -> RDD b
mapRDD = undefined

-- | Process RDD and collect the data
-- This is where all the processing is initiated. The RDD is reduced
-- to number of stages. Typically these stages are 'mapping' and
-- 'reducing'. They are characterized by the fact that mapping can be
-- pipelined, whereas reduce stage typically would need a shuffle in
-- between. 
collect :: Context -> RDD a -> IO [a]
collect sc rdd = undefined




