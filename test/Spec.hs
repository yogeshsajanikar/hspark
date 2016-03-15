module Main(main) where

import Test.QuickCheck
import Test.Framework (defaultMain)
import Test.Framework.Providers.QuickCheck2
import Test.Framework.Providers.HUnit
import Spark.RDD
import LocalSpecs
import StageSpec
import MapSpec

-- | Splits into number of partitions, the number of partitions
-- are limited to cardinality of the input. Each partition can
-- have size at least $m$ or $m+1$, where $m = input_length / n$. 
prop_Splits :: (Positive Int,[Int]) -> Property
prop_Splits (Positive n,ps) =
    let l = length ps
        s = splits n ps
        m = min n l
        o = l `div` m
        lcheck xs = length xs == o || length xs == (o+1) 
    in ps == (concat s) .&&. m == length s .&&. and (map lcheck s)

    
    
main :: IO ()
main = do
  t <- testTransport 
  defaultMain [
         testProperty "splits" prop_Splits
       , testCase "seed rdd" (stageTest t)
       , testCase "map rdd" (mapTest t)
       -- , testCase "square map" sqMapTest
       ]
           
