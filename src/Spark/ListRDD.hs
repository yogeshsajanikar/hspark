{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE KindSignatures #-}

module Spark.ListRDD where

    
import Spark.Context
import Spark.RDD
import Spark.Block

import Control.Distributed.Process
import Control.Distributed.Process.Closure
import Control.Distributed.Static
import Control.Distributed.Process.Serializable
import qualified Data.Map as M
import Data.Typeable
import GHC.Generics
import Data.Binary
import Control.Monad

    

data ListRDD b = ListRDD { _listP :: Partitions b }

-- | Create an empty RDD
emptyRDD :: Serializable a => Context -> ListRDD a
emptyRDD _ = ListRDD emptyP

-- | Create RDD from the data itself
fromListRDD :: Serializable a => Context -> Int -> [a] -> ListRDD a
fromListRDD _ n xs = ListRDD $ Partitions partitions
    where
      partitions = M.fromList ps
      ps = zip [0..] $ splits n xs


instance Typeable b => RDD ListRDD b where

    exec sc (ListRDD ps) = return ps

    flow sc (ListRDD ps) = do
      undefined

-- | Seed RDD

data SeedRDD b = SeedRDD { _divisions :: Int
                         ,_seed :: Closure [b]
                         ,_dict :: Static (SerializableDict [b] )
                         }

partitions :: Context -> Maybe Int -> Int
partitions sc Nothing | n <= 0    =  error "Number of partitions must be > 0"
                      | otherwise = n
    where 
      n = length . slaveNodes . _strategy $ sc
partitions sc (Just n) | n <= 0    = error "Number of partitions must be > 0"
                       | otherwise = n

seedRDD :: Context -> Maybe Int -> Static (SerializableDict [a]) -> Closure [a] -> SeedRDD a
seedRDD sc ps dict inps = SeedRDD n inps dict
    where
      n = partitions sc ps

newtype Fetch a = Fetch ProcessId
    deriving (Typeable, Generic)

instance Serializable a =>  Binary (Fetch a)

matchSeed :: SerializableDict a -> (a -> Process b) -> Match b
matchSeed SerializableDict = match

matchFetch :: SerializableDict a -> (Fetch a -> Process b) -> Match b
matchFetch SerializableDict = match

sendSeed :: SerializableDict a -> ProcessId -> a -> Process ()
sendSeed SerializableDict = send

sendFetch :: SerializableDict a -> ProcessId -> Fetch a -> Process ()
sendFetch SerializableDict = send

seed :: SerializableDict [a] -> Process ()
seed sdict = do
  dt <- receiveWait [ matchSeed sdict $ \xs -> return (Just xs)
                    , match $ \() -> return Nothing ]
  case dt of
    Nothing -> return ()
    Just xs -> receiveWait [ matchFetch sdict $ \(Fetch pid) -> do
                               sendSeed sdict pid xs
                               return ()
                           , match $ \() -> return () 
                           ]

remotable ['seed ]

seedClosure :: Serializable a => Static (SerializableDict [a]) -> Closure (Process ())
seedClosure sdict = staticClosure ($(mkStatic 'seed) `staticApply` sdict)

instance Serializable b => RDD SeedRDD b where

    exec = undefined

    flow sc (SeedRDD n seeds dict) = do
      say "Opening seed data"
      inpData <- unClosure seeds
      
      let ps = splits n inpData
          slaves = slaveNodes . _strategy $ sc
          slavedata = zip (concat (repeat slaves)) ps

      -- Each partition should be associated with one node. However,
      -- allow reusing a nodes if number of partitions are more
      --
      pids <- forM slavedata $ \(nid, dt) -> do
          pid <- spawn nid (seedClosure dict)
          send pid dt 
          return pid

      return $ Blocks $ M.fromList (zip [0..] pids)
