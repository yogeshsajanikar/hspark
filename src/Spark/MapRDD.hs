{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE KindSignatures #-}

module Spark.MapRDD where

    
import Spark.Context
import Spark.RDD
import Spark.SeedRDD
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

-- | RDD representing a pure map between a base with a function
data MapRDD a b c = MapRDD { _baseM :: a b
                           , _cFunM :: Closure (b -> c)
                           , _tdict :: Static (SerializableDict [c])
                           }


-- | Create map RDD from a function closure and base RDD
mapRDD :: (RDD a b, Serializable c) =>
          Context
       -> a b
       -> Static (SerializableDict [c])
       -> Closure (b -> c)
       -> MapRDD a b c
mapRDD sc base dict action = MapRDD { _baseM = base, _cFunM = action, _tdict = dict }


rddMap :: SerializableDict [a]
       -> SerializableDict [b]
       -> (Int, ProcessId)
       -> (a -> b)
       -> Process ()
rddMap dictA dictB (i, pid) f = do
  thispid <- getSelfPid 
  sendFetch dictA pid (Fetch thispid) 
  dt <- receiveWait [ matchSeed dictA $ \xs -> return (Just xs)
                    , match $ \() -> return Nothing
                    ]
  let mapped = fmap f <$> dt
  case mapped of
    Just xs -> receiveWait [ matchFetch dictB $ \(Fetch sender) -> sendSeed dictB sender xs
                           , match $ \() -> return ()
                           ]
    Nothing -> return ()

partitionPair :: SerializableDict (Int, ProcessId)
partitionPair = SerializableDict


remotable [ 'rddMap, 'partitionPair ]

rddMapClosure :: (Serializable a, Serializable b)  =>
                 Static (SerializableDict [a])
              -> Static (SerializableDict [b])
              -> (Int, ProcessId)
              -> Closure (a -> b)
              -> Closure (Process ())
rddMapClosure dictA dictB partition cfun =
    closure decoder (encode partition) `closureApply` cfun
    where
      decoder = ( $(mkStatic 'rddMap) `staticApply` dictA `staticApply` dictB )
                `staticCompose`
                     staticDecode $(mkStatic 'partitionPair)

instance (RDD a b, Serializable c) => RDD (MapRDD a b) c where

    exec sc mr = case unclosure (_lookupTable sc) (_cFunM mr) of
                   Right f -> do
                     ps <- exec sc (_baseM mr)
                     return $ f <$> ps
                   Left e  -> error e

    flow sc (MapRDD base cfun tdict) = do
      -- Get the process IDs of the base process
      (Blocks pmap) <- flow sc base

      -- For each process, try to spawn process on the same node.
      mpids <- forM (M.toList pmap) $ \(i, pid) -> do
                  (Just pi) <- getProcessInfo pid
                  spawn (infoNode pi) (rddMapClosure (rddDictS base) tdict (i, pid)  cfun )
                        
      return $ Blocks $ M.fromList (zip [0..] mpids)
