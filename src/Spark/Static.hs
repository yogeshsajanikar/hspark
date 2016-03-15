{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE TemplateHaskell #-}
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
import Control.Distributed.Process.Closure
import Control.Distributed.Process.Serializable
import Control.Distributed.Static
import Data.Rank1Typeable
import Data.Rank1Dynamic
import Spark.Block
import Data.Typeable
import Data.Binary
import Data.ByteString.Lazy

stageDict :: SerializableDict a -> (ProcessId, a) -> Process ()
stageDict SerializableDict = stage

stageDictStatic :: Static (SerializableDict a -> (ProcessId, a) -> Process ())
stageDictStatic = staticLabel "$stage"

-- remoteTable =
--     registerStatic "$stage"  (toDynamic (stageDict :: SerializableDict ANY -> (ProcessId, ANY) -> Process ()))
--   $ initRemoteTable

remotable ['stageDictStatic]

stageClosure :: Typeable a => Static (SerializableDict a) -> Closure ((ProcessId, a) -> Process ())
stageClosure dict = staticClosure (decoder dict)
    where
      decoder :: Static (SerializableDict a) -> Static ((ProcessId, a) -> Process ())
      decoder dict = (stageDictStatic `staticApply` dict)
