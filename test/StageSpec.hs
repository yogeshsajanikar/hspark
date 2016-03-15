{-# LANGUAGE TemplateHaskell #-}

module StageSpec where

import Spark.Context
import Spark.Block
--import Spark.Static
import Spark.SeedRDD
import Spark.RDD

import Control.Distributed.Process
import Control.Distributed.Process.Closure
import Control.Distributed.Process.Node
--import Control.Distributed.Static
import Network.Transport.TCP (createTransport, defaultTCPParameters)

import Test.Framework.Providers.HUnit
import Test.HUnit

import Control.Concurrent
import Control.Concurrent.MVar


iDict :: SerializableDict [Int]
iDict = SerializableDict 
    
input :: [Int] -> [Int]
input = id

remotable ['iDict, 'input]
    
stageRemoteTable = Spark.SeedRDD.__remoteTable
                 . StageSpec.__remoteTable
                 $ initRemoteTable

stageTest =
    let dt = [1..10] :: [Int]
    in do
      Right t <- createTransport "127.0.0.1" "10501" defaultTCPParameters
      node  <- newLocalNode t stageRemoteTable
      slave0 <- newLocalNode t stageRemoteTable
      slave1 <- newLocalNode t stageRemoteTable
      sc    <- createContextFrom stageRemoteTable (localNodeId node) [localNodeId slave0, localNodeId slave1]
      out   <- newEmptyMVar 
      runProcess node $ do
         let srdd = seedRDD sc (Just 2) $(mkStatic 'iDict)  ( $(mkClosure 'input) dt)
         thispid <- getSelfPid
         (Blocks pmap) <- flow sc srdd
         mapM_ (\ pid ->
            sendFetch (SerializableDict :: SerializableDict [Int]) pid (Fetch thispid) ) pmap
         xss <- mapM (\ _ ->
            receiveWait [
             matchSeed (SerializableDict :: SerializableDict [Int]) $ \xs -> do
               --say $ "Length : " ++ show (length xs)
               return xs ] ) pmap
         mapM_ (\ pid -> send pid () ) pmap
         liftIO $ threadDelay 100000
         liftIO $ putMVar out (concat xss)

      os <- takeMVar out
      putStrLn $ show os
      dt @=? os
            

      
      
