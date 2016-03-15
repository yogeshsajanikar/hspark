{-# LANGUAGE StaticPointers #-}

module LocalSpecs where

import Spark.Context
import Spark.RDD

import Control.Distributed.Process
import Control.Distributed.Static
import Test.Framework.Providers.HUnit
import Test.HUnit

-- square :: Int -> Int
-- square x = x * x

-- staticSquare :: Closure (Int -> Int)
-- staticSquare = staticClosure $ staticPtr $ static square

-- sqMapTest =
--     let dt = [1..10]
--         sqdt = map square dt
--     in do
--       sc <- defaultContext
--       let rdd = fromSeedRDD sc 2 dt
--           mdd = mapRDD sc rdd staticSquare 
--       xdr <- collectP <$> exec sc mdd
--       sqdt @=? xdr


