{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE StaticPointers #-}
module Spark.XDD where

import Control.Distributed.Process
import Data.Typeable
import Spark.Context
import Control.Distributed.Static

-- | XDD represents distributable data set
-- Each data set can be executed 
class Typeable b => XDD a b where

    exec :: Context -> a b -> [b]

-- | Create pure data set
data BDD b = BEmpty 
           | BDD [b]

instance Typeable b => XDD BDD b where

    exec sc (BEmpty) = []
    exec sc (BDD xs) = xs

-- | Mapping Dataset
-- A data set that is dependent upon other data set and can be
-- computed by applying a closure.
data MapBDD a b c =  MapBDD (a b) (Closure (b -> c))

-- | Mapping dataset instance
-- It tries to evaluate the base dataset first, and then executes
-- itself. 
instance (XDD a b, Typeable c) => XDD (MapBDD a b) c where

    exec sc (MapBDD base act) = case unclosure (_lookupTable sc) act of
                                  Right f -> map f $ exec sc base
                                  _ -> error "not implemented"

-- | Create a mapping dataset from a dependent dataset and a map
xmap :: XDD a b => a b -> Closure (b -> c) -> MapBDD a b c
xmap base action = MapBDD base action


