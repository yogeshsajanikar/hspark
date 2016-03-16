{-# LANGUAGE KindSignatures, ScopedTypeVariables, TemplateHaskell #-}
{-# LANGUAGE DeriveGeneric, DeriveDataTypeable, GADTs #-}
{-# LANGUAGE FlexibleInstances, FlexibleContexts #-}

{-# LANGUAGE MultiParamTypeClasses #-}
-- {-# LANGUAGE TemplateHaskell #-}
-- {-# LANGUAGE KindSignatures #-}
-- {-# LANGUAGE RankNTypes #-}

module Spark.ReduceRDD

where

    
import Spark.Context
import Spark.RDD
import Spark.SeedRDD hiding (__remoteTable)
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

data ReduceRDD a k v b = ReduceRDD { _baseM :: a (k,v)
                                   , _cFun  :: Closure (v -> v -> v)
                                   , _pFun  :: Closure (k -> Int)
                                   , _tdict :: Static (SerializableDict [(k,v)])
                                   , _kdict :: Static (OrdDict k)
                                   }


-- | ReduceRDD takes a base RDD that produces a pair, and reduces it per key
-- ReduceRDD uses a combining function and every block is reduced
-- using combining function. The reduction happens in two steps. In
-- the first step, reduction is done per block. In the next iteration,
-- the hashing (partitioning) function is applied to shuffle the
-- locally reduced data to only one node. Further reduction is done
-- per such hashed block. 
reduceRDD :: (RDD a (k,v), Ord k, Serializable k, Serializable v) =>
             Context
          -> a (k,v)
          -> Static (OrdDict k)
          -> Static (SerializableDict [(k,v)] )
          -> Closure (v -> v -> v)
          -> Closure (k -> Int)
          -> ReduceRDD a k v (k,v)
reduceRDD sc base dictk dict combiner partitioner =
    ReduceRDD base combiner partitioner dict dictk


data FetchPartition = FetchPartition Int ProcessId
                      deriving (Typeable, Generic)

instance Binary FetchPartition

sendKV :: SerializableDict [(k,v)] -> ProcessId -> [(k,v)] -> Process ()
sendKV SerializableDict = send

data OrdDict a where
    OrdDict :: forall a . Ord a => OrdDict a

rFromList :: OrdDict k -> (v -> v -> v) -> [(k,v)] -> M.Map k v
rFromList OrdDict =  M.fromListWith

rUnion :: OrdDict k -> (v -> v -> v) -> M.Map k v -> M.Map k v -> M.Map k v
rUnion OrdDict = M.unionWith

reduceStep1 :: OrdDict k
            -> SerializableDict [(k,v)]
            -> (Int, ProcessId)
            -> (v -> v -> v)
            -> (k -> Int)
            -> Process ()
reduceStep1 dictk dictkv (n, pid) combiner partitioner = do
  thispid <- getSelfPid
  sendFetch dictkv pid (Fetch thispid)
  dt <- receiveWait [ matchSeed dictkv $ \xs -> return (Just xs)
                    , match $ \() -> return Nothing
                    ]
  -- Reduction step 1, locally reduce the block
  let mp = case dt of
             Just xs -> rFromList dictk combiner xs
             Nothing -> M.empty

  -- Serve all the keys for given partition.
  receiveWait [ match $ \(FetchPartition p sid) -> do
                  let kvs = M.toList $ M.filterWithKey (\k _ -> partitioner k `mod` n == p ) mp
                  sendKV dictkv sid kvs
              , match $ \() -> return () 
              ]

expectKV :: SerializableDict [(k,v)] -> Process [(k,v)]
expectKV SerializableDict = expect

matchKV :: SerializableDict [(k,v)] -> ([(k,v)] -> Process b) -> Match b
matchKV SerializableDict = match

-- | Reduction Step 2 : Get all the 
reduceStep2 :: OrdDict k
            -> SerializableDict [(k,v)]
            -> (Int, [(Int, ProcessId)]) -- ^ Partition and all the
                                         -- processes in reduction
                                         -- step 1
            -> (v -> v -> v)       -- ^ Combiner function
            -> Process ()
reduceStep2 dictk dictkv (p, ips) combiner = do
  thispid <- getSelfPid
  let kvreduce mp (i,pid) = do
        send pid (FetchPartition p thispid)
        vs <- receiveWait [ matchKV dictkv $ \kvs -> return kvs ]
        let mp1 = rFromList dictk combiner vs
        return $ rUnion dictk combiner mp mp1
  reduced <- foldM kvreduce M.empty ips

  -- Now that we have
  let kvs = M.toList reduced
  receiveWait [ matchFetch dictkv $ \(Fetch pid) -> do
                  sendSeed dictkv pid kvs
                  return ()
              , match $ \() -> return ()
              ]

partitionedPids :: SerializableDict (Int, [(Int, ProcessId)])
partitionedPids = SerializableDict

partitionPair :: SerializableDict (Int, ProcessId)
partitionPair = SerializableDict

remotable [ 'reduceStep1, 'reduceStep2, 'partitionedPids, 'partitionPair ]

reduceStep1Closure :: (Ord k, Serializable k, Serializable v) =>
                      Static (OrdDict k)
                   -> Static (SerializableDict [(k,v)])
                   -> (Int, ProcessId)
                   -> Closure (v -> v -> v)
                   -> Closure (k -> Int)
                   -> Closure (Process ())
reduceStep1Closure dictk dictkv ipid combiner partitioner =
    closure decoder (encode ipid) `closureApply` combiner `closureApply` partitioner
        where
          decoder = ( $(mkStatic 'reduceStep1) `staticApply` dictk `staticApply` dictkv )
                    `staticCompose` staticDecode $(mkStatic 'partitionPair)

reduceStep2Closure :: (Ord k, Serializable k, Serializable v) =>
                      Static (OrdDict k)
                   -> Static (SerializableDict [(k,v)])
                   -> (Int, [(Int, ProcessId)])
                   -> Closure (v -> v -> v)
                   -> Closure (Process ())
reduceStep2Closure dictk dictkv ipid combiner =
    closure decoder (encode ipid) `closureApply` combiner 
        where
          decoder = ( $(mkStatic 'reduceStep2) `staticApply` dictk `staticApply` dictkv )
                    `staticCompose` staticDecode $(mkStatic 'partitionedPids)


instance (Ord k, Serializable k, Serializable v, RDD a (k,v)) => RDD (ReduceRDD a k v) (k,v) where

    exec = undefined

    rddDictS mr = _tdict mr

    rddDict _ = SerializableDict
                
    flow sc (ReduceRDD base combiner partitioner dictkv dictk) = do
        -- Get the process IDs of the base process
        (Blocks pmap) <- flow sc base

        let slaves = slaveNodes . _strategy $ sc
            p = M.size pmap -- Size of the partitions
            n = length slaves


        -- Do two step reduction
        -- In the first step, do local reduction, i.e. 
        mpids <- forM (M.toList pmap) $ \(i, pid) -> do
                    (Just pi) <- getProcessInfo pid
                    spawn (infoNode pi) (reduceStep1Closure dictk dictkv (p, pid) combiner partitioner)

        -- For the second step, all the process ids are sent to 
        let step1pids  = zip [0..] mpids 
            slavenodes = zip [0..] (take p $ concat (repeat slaves)) 

        -- for each node now, call the reduction step 2.
        -- This involves shuffling across the nodes.
        rpids <- forM slavenodes $ \(i, nid) -> do
                   spawn nid (reduceStep2Closure dictk dictkv (i, step1pids) combiner)

        return $ Blocks $ M.fromList (zip [0..] rpids)
