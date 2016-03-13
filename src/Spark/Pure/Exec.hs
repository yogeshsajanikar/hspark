{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}
module Spark.Pure.Exec where


import Spark.RDD
import Spark.Context
import qualified Data.Map as M
import Control.Distributed.Process
import Control.Distributed.Process.Serializable
import Control.Distributed.Static
import Data.Typeable
    
executePure :: RDD a -> IO [a]
executePure rdd = undefined
    -- case rdd of
    --   RDD (Partitions ps) -> return . concat . map snd . M.toList $ ps
    --   MapRDD act base ->  case unclosure undefined act of
    --                         Right f -> map f <$> executePure base
    --   _ -> undefined


