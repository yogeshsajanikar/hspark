{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE StaticPointers #-}

module ReduceSpec where

import Spark
import qualified Data.Map as M
import Data.List (sortBy)

import Control.Distributed.Process
import Control.Distributed.Static hiding (initRemoteTable)
import Control.Distributed.Process.Closure
import Control.Distributed.Process.Node
import Network.Transport.TCP (createTransport, defaultTCPParameters)

import Test.Framework.Providers.HUnit
import Test.HUnit

import Control.Concurrent
import Control.Concurrent.MVar

import System.Random

-- For output
iDictKV :: SerializableDict [(Int,Int)]
iDictKV = SerializableDict

-- Key requires ordering
dictK :: OrdDict Int
dictK = OrdDict

-- Add the values to find the sum
combine :: Int -> Int -> Int
combine = (+)

-- Generated inputs are in the same range, just return the same key
-- Else, we should have been including hash
partitioner :: Int -> Int
partitioner = id
              
input :: [(Int,Int)] -> [(Int,Int)]
input = id

remotable ['iDictKV, 'dictK, 'input, 'partitioner, 'combine]
    
mapRemoteTable = Spark.remoteTable
               . ReduceSpec.__remoteTable
               $ initRemoteTable

createNodes t = do
      node   <- newLocalNode t mapRemoteTable
      slave0 <- newLocalNode t mapRemoteTable
      slave1 <- newLocalNode t mapRemoteTable
      return (node, [slave0, slave1])


closeContext (master, slaves)  = do
  mapM_ closeLocalNode slaves
  closeLocalNode master

generateInput :: Int -> (Int, Int) -> IO [(Int,Int)]
generateInput n (l,h) = do
  seed <- newStdGen
  let ints = take n $ randomRs (l,h) seed
  return $ zip ints (repeat 1)
    
reduceTest t = do
  (master, slaves)  <- createNodes t
  inp <- generateInput 100 (1,10)
  sc  <- createContextFrom mapRemoteTable (localNodeId master) (localNodeId <$> slaves)
  out <- newEmptyMVar 
  runProcess master $ do
      let dictkvS = $(mkStatic 'iDictKV)
          dictkS  = $(mkStatic 'dictK)
          inputC  = ($(mkClosure 'input) inp)
          combC   = staticClosure $(mkStatic 'combine)
          partC   = staticClosure $(mkStatic 'partitioner)
          
          seed = seedRDD sc (Just 2) dictkvS inputC 
          redx = reduceRDD sc seed dictkS dictkvS combC partC

          dict = SerializableDict :: SerializableDict [(Int,Int)]

      output <- collect sc dict redx
      liftIO $ putMVar out output
      liftIO $ putStrLn $ show output

  closeContext (master, slaves)
  os <- takeMVar out
  let expected  = M.toList $ M.fromListWith (+) inp
      actual = sortBy ( \(k1,_) (k2,_) -> compare k1 k2) os

  expected @=? actual
