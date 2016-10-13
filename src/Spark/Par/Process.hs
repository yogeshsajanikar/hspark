{-# LANGUAGE ImpredicativeTypes #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE DeriveGeneric #-}
module Spark.Par.Process where

import Control.Distributed.Process
import Control.Distributed.Static
import Control.Distributed.Process.Closure
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


sdictFetch :: SerializableDict a -> SerializableDict (Fetch a)
sdictFetch SerializableDict = SerializableDict 

expectS :: SerializableDict a -> Process a
expectS SerializableDict = expect

sendChanS :: SerializableDict a -> SendPort a -> a -> Process ()
sendChanS SerializableDict = sendChan

-- | Process that stores the data, until terminated explicitly
storeProc :: SerializableDict a -> a -> Process ()
storeProc dA x = do
  f <- expectS (sdictFetch dA)
  case f of
    Fetch pid sport -> do
      sendChanS dA sport x
      storeProc dA x
    Terminate ->
      return ()

-- | Given a process, store its value in a locally spawned store 
createStore :: SerializableDict a -> Process a -> Process ProcessId
createStore dA proc_ = do
   x <- proc_
   spawnLocal (storeProc dA x)

$(remotable [ 'storeProc, 'createStore ])



