module Spark.Par.Convert where

import Spark.Par.Types
import Control.Distributed.Process

-- rddToPar (DataRDD bs) = mapM blockPar bs
--   where
--     blockPar (Block p) = do
--       i <- new
--       put i p
--       return i
      
rddToPar _ = undefined


  
