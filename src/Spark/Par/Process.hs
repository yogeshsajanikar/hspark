{-# LANGUAGE DeriveGeneric #-}
module Spark.Par.Process where

import Control.Distributed.Process
import Control.Distributed.Process.Async
import Data.Binary
import Data.Typeable
import GHC.Generics
import Spark.Par.Types

data Fetch a = Fetch ProcessId (SendPort a)
             | Terminate
             deriving Generic

instance (Typeable a, Binary a) => Binary (Fetch a)

data Packet a = Packet ProcessId a
              | EmptyPacket ProcessId

-- | Process that stores the data, until terminated explicitly
storeProc :: (Typeable a, Binary a) => a -> Process ()
storeProc x = do
  f <- expect
  case f of
    Fetch pid sport -> do
      sendChan sport x
      storeProc x
    Terminate ->
      return ()


createStore :: (Binary a, Typeable a) => Process a -> Process ProcessId
createStore proc_ = do
  x <- proc_
  spawnLocal (storeProc x)
