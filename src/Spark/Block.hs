{-# LANGUAGE DeriveGeneric #-}
-- |
-- Module : Spark.Block
--
-- Each block is defined by a block of data residing in a process. We
-- spawn the process on remote nodes, and send the data. Similarly a
-- mapping stage defines a process that fetches the data from
-- dependent process and holds it till asked by master or another
-- process. 
module Spark.Block where

import Control.Distributed.Process
import Control.Distributed.Process.Serializable
import Control.Distributed.Static
import qualified Data.Map as M
import Control.Monad
import Data.Typeable
import Data.Binary
import GHC.Generics
import qualified Data.ByteString.Lazy as BL


-- | Partitions represent a data encapsulated in a process
-- The data can be requested by requesting the process.

newtype Blocks a = Blocks { _blocks :: M.Map Int ProcessId }

-- | Request the data from a process
newtype RequestData = RequestData ProcessId
    deriving (Typeable, Generic)

instance Binary RequestData
    
-- | Send the data back
newtype BlockData a = PD a
    deriving (Typeable, Generic)

instance Serializable a => Binary (BlockData a)

-- | Stage the data in a process
-- Stage the data in the process such that it can be fetched by an
-- appropriate message. 

stage :: Serializable a => ProcessId -> a -> Process ()
stage master dt = do
  pid <- getSelfPid
  say "Data received .."
  send master pid

  let sendData (RequestData pid)= send pid (PD dt)

  -- Be ready to serve the data
  forever $ receiveWait [ match sendData ]


-- | Process data with a closure map

mapStage :: (Serializable a, Serializable b)
            => Closure (ProcessId, ProcessId) -> Closure (a -> b)  -> Process ()
mapStage cs cf = do
  (master, source) <- unClosure cs
  pid <- getSelfPid
  send source (RequestData pid)
  -- Wait till we receive the data
  let receiveData (PD xs) = return xs
  dt <- receiveWait [ match receiveData ]
  f  <- unClosure cf
  stage master (f dt)
  

-- | Process data, combine it with IO closure map.

mapStageIO :: (Serializable a, Serializable b)
           => Closure (ProcessId, ProcessId)
           -> Closure (a -> IO b)
           -> Process ()
mapStageIO cs cf = do
  (master, source) <- unClosure cs
  pid <- getSelfPid
  send source (RequestData pid)
  -- Wait till we receive the data
  let receiveData (PD xs) = return xs
  dt <-  receiveWait [ match receiveData ]
  f  <-  unClosure cf
  pdt <- liftIO $ f dt
  stage master pdt

