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


class Typeable b => XDD a b where

    exec :: Context -> a b -> [b]



data BDD b = BEmpty 
           | BDD [b]

instance Typeable b => XDD BDD b where

    exec sc (BEmpty) = []
    exec sc (BDD xs) = xs

data MapBDD a b c =  MapBDD (a b) (Closure (b -> c))


instance (XDD a b, Typeable c) => XDD (MapBDD a b) c where

    exec sc (MapBDD base act) = case unclosure undefined act of
                                  Right f -> map f $ exec sc base
                                  _ -> undefined


data SDD a b = SDD

cpMap :: XDD a b => a b -> Closure (b -> c) -> MapBDD a b c
cpMap base action = MapBDD base action


sdMap :: SDD a b -> SDD b c -> SDD a c
sdMap = undefined 
