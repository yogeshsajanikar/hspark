{-# LANGUAGE GADTs #-}
-- |
-- Module : Spark.Static
--
-- Each block is defined by a block of data residing in a process. We
-- spawn the process on remote nodes, and send the data. Similarly a
-- mapping stage defines a process that fetches the data from
-- dependent process and holds it till asked by master or another
-- process.
--
module Spark.Static where

import Control.Distributed.Process
import Control.Distributed.Process.Serializable
import Control.Distributed.Static
import Data.Rank1Typeable
import Data.Rank1Dynamic
import Spark.Block
import Data.Typeable
import Data.Binary
import Data.ByteString.Lazy

data BinaryDict a where
  BinaryDict :: Binary a => BinaryDict a

stageDict :: Typeable a => BinaryDict a -> (ProcessId, a) -> Process ()
stageDict BinaryDict = stage

decodeDict :: BinaryDict a -> ByteString -> a
decodeDict dict = decode

stageDictStatic :: Static (BinaryDict a -> (ProcessId, a) -> Process ())
stageDictStatic = staticLabel "$stage"

decodeDictStatic :: Static (BinaryDict a -> ByteString -> a)
decodeDictStatic = staticLabel "$decode" 

remoteTable =
    registerStatic "$stage"  (toDynamic (stageDict :: BinaryDict ANY -> (ProcessId, ANY) -> Process ()))
  . registerStatic "$decode" (toDynamic (decodeDict :: BinaryDict ANY -> ByteString -> ANY))
  $ initRemoteTable


stageClosure :: Serializable a => Static (BinaryDict a) -> Closure ((ProcessId, a) -> Process ())
stageClosure dict = staticClosure decoder 
    where
      --decoder :: Static (ByteString -> (ProcessId, a) -> Process () )
      decoder = (stageDictStatic `staticApply` dict) 
