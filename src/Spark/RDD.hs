{-# LANGUAGE ExistentialQuantification #-}
module Spark.RDD

where

import Spark 

-- | Partition is a list of values     
data Partition a = Partition { values :: [a] }
                   deriving Show


-- | Dependencies of a RDD
class Dependencies a wherea
    
    rdd :: RDD a

    cached :: Maybe [RDD a]

-- | RDD is a collection of partitions
-- Partitions can be distributed.
data RDD a = forall b . Dependency b => RDD { partitions :: [Partition a]
                                            , context :: Context
                                            , dependencies :: b }


