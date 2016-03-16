{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE StaticPointers #-}

module MapSpec where

import Spark
import Data.List (sort)

import Control.Distributed.Process
import Control.Distributed.Static hiding (initRemoteTable)
import Control.Distributed.Process.Closure
import Control.Distributed.Process.Node
import Network.Transport.TCP (createTransport, defaultTCPParameters)

import Test.Framework.Providers.HUnit
import Test.HUnit

import Control.Concurrent
import Control.Concurrent.MVar


iDict :: SerializableDict [Int]
iDict = SerializableDict


square :: Int -> Int
square x = x * x

staticSquare :: Closure (Int -> Int)
staticSquare = staticClosure $ staticPtr $ static square
    
input :: [Int] -> [Int]
input = id

remotable ['iDict, 'input]
    
mapRemoteTable = Spark.remoteTable
               . MapSpec.__remoteTable
               $ initRemoteTable

mapTest t =
    let dt = [1..10] :: [Int]
    in do
      node  <- newLocalNode t mapRemoteTable
      slave0 <- newLocalNode t mapRemoteTable
      slave1 <- newLocalNode t mapRemoteTable
      sc    <- createContextFrom mapRemoteTable (localNodeId node) [localNodeId slave0, localNodeId slave1]
      out   <- newEmptyMVar 
      runProcess node $ do
         let srdd = seedRDD sc (Just 2) $(mkStatic 'iDict)  ( $(mkClosure 'input) dt)
             mrdd = mapRDD sc srdd $(mkStatic 'iDict) staticSquare
             dict = SerializableDict :: SerializableDict [Int]

         output <- collect sc dict mrdd
         liftIO $ putMVar out output
         liftIO $ putStrLn $ show output
         liftIO $ threadDelay 100000

      closeLocalNode slave1
      closeLocalNode slave0
      closeLocalNode node

      os <- takeMVar out
      let squares = map square dt
      (sort squares) @=? (sort os)
            

      
      
