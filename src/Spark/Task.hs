{-# LANGUAGE RankNTypes #-}
module Spark.Task where

import Control.Distributed.Process hiding (call)
import Control.Distributed.Process.Serializable
import Control.Distributed.Process.Task.Queue.BlockingQueue

import qualified Data.Map as M

data Partitions m a = Partitions { _ps :: M.Map Int (m a) }

data CPartitions m a = CPartitions { _cps :: M.Map Int (Closure (m a) ) }

data BDD a = BDD (Partitions [] a)

bcollect :: Monad m => Partitions m a -> Partitions Closure (m a)
bcollect = undefined
