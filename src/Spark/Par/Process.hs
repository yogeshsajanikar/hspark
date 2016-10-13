{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE DeriveGeneric #-}
module Spark.Par.Process where

import Data.ByteString.Lazy
import Control.Distributed.Process
import Control.Distributed.Static
import Control.Distributed.Process.Closure
import Control.Distributed.Process.Async
import Control.Distributed.Process.Serializable
import Data.Binary
import Data.Typeable
import GHC.Generics
import Spark.Par.Types
import Data.Rank1Typeable
import Data.Rank1Dynamic

data Fetch a = Fetch ProcessId (SendPort a)
             | Terminate
             deriving Generic

instance (Typeable a, Binary a) => Binary (Fetch a)

-- | Represents data existing at process id 
newtype RemoteData a = RemoteData ProcessId
  deriving Generic

instance (Typeable a, Binary a) => Binary (RemoteData a)


-- | Process that stores the data, until terminated explicitly
storeProc :: Serializable a => a -> Process ()
storeProc x = do
  f <- expect
  case f of
    Fetch pid sport -> do
      sendChan sport x
      storeProc x
    Terminate ->
      return ()

decodeDict :: SerializableDict a -> ByteString -> a
decodeDict SerializableDict = decode

decodeStatic :: Static (SerializableDict a -> ByteString -> a)
decodeStatic = staticLabel "$decode"

createStore :: Serializable a => a -> Process (RemoteData a)
createStore x = spawnLocal (storeProc x) >>= return . RemoteData

createStoreDict :: SerializableDict a -> a -> Process (RemoteData a)
createStoreDict SerializableDict = createStore

createStoreStatic :: Static (SerializableDict a -> a -> Process (RemoteData a))
createStoreStatic = staticLabel "$createStore"

createStoreClosure :: Binary a => Static (SerializableDict a) -> a -> Closure (Process (RemoteData a))
createStoreClosure dict x = closure decoder (encode x)
  where
    -- decoder :: Static (ByteString -> Process (RemoteData a))
    decoder = (createStoreStatic `staticApply` dict) `staticCompose` (decodeStatic `staticApply` dict)

listDict :: forall a . SerializableDict a -> SerializableDict [a]
listDict SerializableDict = SerializableDict

listDictStatic :: Static (SerializableDict a -> SerializableDict [a])
listDictStatic = staticLabel "$listDict"

listDictClosure :: Static (SerializableDict a) -> Static (SerializableDict [a])
listDictClosure sdict = listDictStatic `staticApply` sdict

remoteDict :: forall a . SerializableDict a -> SerializableDict (RemoteData a)
remoteDict SerializableDict = SerializableDict

remoteDictStatic :: Static (SerializableDict a -> SerializableDict (RemoteData a))
remoteDictStatic = staticLabel "$remoteDict"

remoteDictClosure ::Static (SerializableDict a) -> Static (SerializableDict (RemoteData a))
remoteDictClosure sdict = remoteDictStatic `staticApply` sdict

rtable = registerStatic "$decode" (toDynamic (decodeDict :: SerializableDict ANY -> ByteString -> ANY))
  . registerStatic "$createStore" (toDynamic (createStoreDict :: SerializableDict ANY -> ANY -> Process (RemoteData ANY)))
  . registerStatic "$listDict" (toDynamic (listDict :: SerializableDict ANY -> SerializableDict [ANY]))
  . registerStatic "$remoteDict" (toDynamic (remoteDict :: SerializableDict ANY -> SerializableDict (RemoteData ANY)))
  $ initRemoteTable

