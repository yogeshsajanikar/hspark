-- |
-- Module: Spark.Stage
--
module Spark.Stage where

import Spark.RDD
import Control.Distributed.Process    

type Stage a = Closure (Partition a)
        
distribute :: Closure (Partitions a) -> Partitions (Closure a)
distribute = undefined

             
