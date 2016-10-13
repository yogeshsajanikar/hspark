{-# LANGUAGE TemplateHaskell #-}
module Spark.Par.Convert where

import Spark.Par.Types
import Control.Distributed.Process
import Control.Distributed.Process.Closure
import Control.Distributed.Static

import Spark.Par.Process

-- | Convert RDD into parallel jobs
-- rddToPar :: RDD a -> NodePar [NIVar (RemoteData [a])]
rddToPar (DataRDD dict bs) = mapM blockPar bs
  where
    blockPar p = do
      i <- new
      put i (t p)
      return i
      
    sdict = listDictStatic `staticApply` dict

    rdict = remoteDictStatic `staticApply` sdict

    t p = AsyncPTask {
      asyncDict = rdict,
      asyncProc = (createStoreClosure sdict p) }
      
rddToPar _ = undefined
