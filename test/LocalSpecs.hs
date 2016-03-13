{-# LANGUAGE StaticPointers #-}

module LocalSpecs where

import Spark.Context
import Spark.XDD

import Control.Distributed.Process
import Control.Distributed.Static
import Test.Framework.Providers.HUnit
import Test.HUnit

square :: Int -> Int
square x = x * x

staticSquare :: Closure (Int -> Int)
staticSquare = staticClosure $ staticPtr $ static square

sqMapTest =
    let dt = [1..10]
        bdd = BDD dt :: BDD Int
        sqbdd = xmap bdd staticSquare
        sqdt = map square dt
    in do
      sc <- defaultContext
      let xdr = exec sc sqbdd
      sqdt @=? xdr

