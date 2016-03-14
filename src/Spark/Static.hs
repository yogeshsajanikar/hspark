{-# LANGUAGE StaticPointers #-}
module Spark.Static where

import Control.Distributed.Process
import Control.Distributed.Process.Serializable
import Control.Distributed.Static
import Spark.Block

cseed :: Serializable a => Closure (ProcessId -> Process a)
cseed = staticClosure $ staticPtr $ static seed
    
