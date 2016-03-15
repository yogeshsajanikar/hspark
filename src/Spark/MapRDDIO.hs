{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE KindSignatures #-}

module Spark.MapRDDIO where

    
import Spark.Context
import Spark.RDD
import Spark.SeedRDD hiding (__remoteTable)
import Spark.MapRDD hiding (__remoteTable)
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
data MapRDDIO a b c = MapRDDIO { _baseM :: a b
                               , _cFunM :: Closure (b -> IO c)
                               , _tdict :: Static (SerializableDict [c])
                           }


-- | Create map RDD from a function closure and base RDD
maprddIO :: (RDD a b, Serializable c) =>
            Context
         -> a b
         -> Static (SerializableDict [c])
         -> Closure (b -> IO c)
         -> MapRDDIO a b c
maprddIO sc base dict action = MapRDDIO { _baseM = base, _cFunM = action, _tdict = dict }


rddIOMap :: SerializableDict [a]
         -> SerializableDict [b]
         -> (Int, ProcessId)
         -> (a -> IO b)
         -> Process ()
rddIOMap dictA dictB (i, pid) f = do
  thispid <- getSelfPid 
  sendFetch dictA pid (Fetch thispid) 
  dt <- receiveWait [ matchSeed dictA $ \xs -> return (Just xs)
                    , match $ \() -> return Nothing
                    ]
  case dt of
    Just ys -> do
            xs <- liftIO $ mapM f ys
            receiveWait [ matchFetch dictB $ \(Fetch sender) -> sendSeed dictB sender xs
                        , match $ \() -> return ()
                        ]
    Nothing -> return ()

partitionIOPair :: SerializableDict (Int, ProcessId)
partitionIOPair = SerializableDict

remotable [ 'rddIOMap, 'partitionIOPair ]

rddIOMapClosure :: (Serializable a, Serializable b)  =>
                   Static (SerializableDict [a])
                -> Static (SerializableDict [b])
                -> (Int, ProcessId)
                -> Closure (a -> IO b)
                -> Closure (Process ())
rddIOMapClosure dictA dictB partition cfun =
    closure decoder (encode partition) `closureApply` cfun
    where
      decoder = ( $(mkStatic 'rddIOMap) `staticApply` dictA `staticApply` dictB )
                `staticCompose`
                     staticDecode $(mkStatic 'partitionIOPair)

instance (RDD a b, Serializable c) => RDD (MapRDDIO a b) c where

    exec sc mr = undefined

    flow sc (MapRDDIO base cfun tdict) = do
      -- Get the process IDs of the base process
      (Blocks pmap) <- flow sc base

      -- For each process, try to spawn process on the same node.
      mpids <- forM (M.toList pmap) $ \(i, pid) -> do
                  (Just pi) <- getProcessInfo pid
                  spawn (infoNode pi) (rddIOMapClosure (rddDictS base) tdict (i, pid)  cfun )

      -- close the process
      mapM_ (\pid -> send pid () ) pmap
                        
      return $ Blocks $ M.fromList (zip [0..] mpids)
