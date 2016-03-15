{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE KindSignatures #-}

module Spark.SeedRDD
    (
     -- * Seed RDD
     SeedRDD(),
     seedRDD,
     -- * Interaction with seeded process
     Fetch (..),
     matchFetch,
     sendFetch,
     matchSeed,
     sendSeed,
     -- * Closures and Seed Process
     seed,
     seedClosure,
     -- * Remote Table
     __remoteTable
    )
where

    
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

    

-- | Seed RDD
-- Typically a starting point of the workflow. Divides the input data
-- into partitions 

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

-- | Seed receives the data, and keeps it for further reference.
-- One can receive the data back when asked for
seed :: SerializableDict [a] -> Process ()
seed sdict = do
  dt <- receiveWait [ matchSeed sdict $ \xs -> do
                        say "Received seed data"
                        return (Just xs)
                    , match $ \() -> do
                        say "Did not receive data, closing"
                        return Nothing ]
  case dt of
    Nothing -> return ()
    Just xs -> receiveWait [ matchFetch sdict $ \(Fetch pid) -> do
                               say "Sending data back"
                               sendSeed sdict pid xs
                               return ()
                           , match $ \() -> do
                               say "Closing seed store"
                               return () 
                           ]

remotable ['seed ]

-- | Closure around seed
-- Seed closure is used spawn the seeding process
seedClosure :: Serializable a => Static (SerializableDict [a]) -> Closure (Process ())
seedClosure sdict = staticClosure ($(mkStatic 'seed) `staticApply` sdict)

instance Serializable b => RDD SeedRDD b where

    exec = undefined

    -- | Serializable dictionary of the target
    rddDict sc = SerializableDict

    -- | Startic decoder
    rddDictS = _dict

    -- | Control flow of the RDD
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
