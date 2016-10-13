module Spark.Par.Run where

import Spark.Par.Types
import Data.IORef
import Control.Concurrent.MVar

-- Modified version of scheduler in monad-par, adapted for distributed
-- haskell. The main difference is that rather than forking a thread,
-- we spawn a process on a node, and await the task to be executed 

data Sched = Sched
  { no       :: {-# UNPACK #-} !Int,
    workpool :: IORef [Plan],
    idle     :: IORef [MVar Bool],
    scheds   :: [Sched] -- Global list of all per-thread workers.
  }

sched :: Bool -> Sched -> Plan -> IO ()
sched = undefined

