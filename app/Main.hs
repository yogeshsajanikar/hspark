{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE StaticPointers #-}

module Main where

import Spark.DSL
import Spark.Par.Process
import Spark.Par.Convert
import Spark.RDD (splits)
import Control.Distributed.Process
import Control.Distributed.Process.Closure
import Control.Distributed.Process.Node
import Network.Transport.TCP (createTransport, defaultTCPParameters)
import Control.Monad

idict :: SerializableDict Int
idict = SerializableDict

$(remotable ['idict])

remoteTable =
  Main.__remoteTable
  $ rtable

defaultTransport = do
  Right t <- createTransport "127.0.0.1" "10501" defaultTCPParameters
  return t
  

main :: IO ()
main = do
  transport <- defaultTransport
  localNodes <- replicateM 5 (newLocalNode transport remoteTable)
  let nodes = map localNodeId localNodes

  let master = head localNodes
      input = splits 10 [1..100]
  vars <- runProcess master $ do
    let dt = DataRDD $(mkStatic 'idict) input
        par = rddToPar dt
    -- ,vars <- runPar (tail nodes) par 
    return ()
  return ()
