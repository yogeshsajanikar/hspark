{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE StaticPointers #-}

module MapSpec where

import Spark.Context
import Spark.Block
import Spark.SeedRDD
import Spark.MapRDD
import Spark.RDD

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
    
mapRemoteTable = Spark.SeedRDD.__remoteTable
               . Spark.MapRDD.__remoteTable
               . MapSpec.__remoteTable
               $ initRemoteTable

mapTest t =
    let dt = [1..10] :: [Int]
    in do
      --Right t <- createTransport "127.0.0.1" "10501" defaultTCPParameters
      node  <- newLocalNode t mapRemoteTable
      slave0 <- newLocalNode t mapRemoteTable
      slave1 <- newLocalNode t mapRemoteTable
      sc    <- createContextFrom mapRemoteTable (localNodeId node) [localNodeId slave0, localNodeId slave1]
      out   <- newEmptyMVar 
      runProcess node $ do
         let srdd = seedRDD sc (Just 2) $(mkStatic 'iDict)  ( $(mkClosure 'input) dt)
             mrdd = mapRDD sc srdd $(mkStatic 'iDict) staticSquare
         thispid <- getSelfPid
         (Blocks pmap) <- flow sc mrdd
         mapM_ (\ pid ->
            sendFetch (SerializableDict :: SerializableDict [Int]) pid (Fetch thispid) ) pmap
         xss <- mapM (\ _ ->
            receiveWait [
             matchSeed (SerializableDict :: SerializableDict [Int]) $ \xs -> do
               --say $ "Length : " ++ show (length xs)
               return xs ] ) pmap
         mapM_ (\ pid -> send pid () ) pmap
         let output = concat xss
         liftIO $ putMVar out output
         liftIO $ putStrLn $ show output
         liftIO $ threadDelay 100000

      os <- takeMVar out
      let squares = map square dt
      squares @=? os
            

      
      
