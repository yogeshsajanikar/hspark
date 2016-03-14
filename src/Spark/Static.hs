{-# LANGUAGE StaticPointers, ConstraintKinds, GADTs, RankNTypes #-}
module Spark.Static where

import Control.Distributed.Process
import Control.Distributed.Process.Serializable
import Control.Distributed.Static
import Spark.Block
import Data.Typeable
import GHC.StaticPtr

--cseed :: (Typeable a, Serializable a) => Closure (ProcessId -> Process a)
--cseed = staticClosure $ staticPtr $ static seed

--cseed :: Typeable a => StaticPtr (ProcessId -> Process a)
--cseed = static seed


sq :: Typeable a => a -> a
sq x = x

data Dict a where
    Dict :: Typeable a . a => Dict a

sqD :: forall a . Dict (Typeable a) -> a -> a
sqD dict = sq

           
func :: forall a . StaticPtr ((Dict a) -> a -> a)
func = static sqD
