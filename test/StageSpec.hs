module StageSpec where

import Spark.Context
import Spark.Block
import Spark.Static

import Control.Distributed.Process
import Control.Distributed.Process.Closure
import Control.Distributed.Static
import Network.Transport.TCP (createTransport, defaultTCPParameters)

iDict :: Static [Int]
iDict = SerializableDict [Int]
    
remoteTable ['iDict]
    
stageRemoteTable = Spark.Static.__remoteTable
                 . __remoteTable
                 $ initRemoteTable

stageTest =
    let dt = [1..10] :: [Int]
    in do
      Right t <- createTransport "127.0.0.1" "10501" defaultTCPParameters
      node <- newLocalNode t initRemoteTable
      runProcess node $ do

         stagePid <- spawn (stageClosure $(mkStatic 'iDict) `
      
