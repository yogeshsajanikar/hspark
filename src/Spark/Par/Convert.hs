module Spark.Par.Convert where

import Spark.Par.Types

rddToPar (DataRDD bs) = mapM blockPar bs
  where
    blockPar (Block p) = do
      i <- new
      put i p
      return i
      
rddToPar _ = undefined


  
