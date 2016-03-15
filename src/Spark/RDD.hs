{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE RankNTypes, GADTs #-}
-- |
-- Module : Spark.RDD
--
-- RDD is represented by indexed partitions. Each partition can be
-- processed parallelly. RDD supports transformations, and grouping
-- operations. Each of these transformations produces another RDD,
-- which can be collected lazily.
--
module Spark.RDD

where

import Spark.Context
import Control.Distributed.Process
import Control.Distributed.Static
import Control.Distributed.Process.Serializable
import qualified Data.Map as M
import Data.Typeable
import Data.Binary
import GHC.Generics
import Control.Monad
import Spark.Block


-- | Partioined data, indexed by integers
newtype Partitions a = Partitions { _dataMaps :: M.Map Int [a] }
    deriving (Show, Generic)

collectP :: Partitions a -> [a]
collectP (Partitions mp) = concat . map snd . M.toList $ mp

instance Functor Partitions where

    f `fmap` (Partitions mp) = Partitions $ fmap (fmap f) mp

emptyP :: Partitions a
emptyP = Partitions M.empty
             
instance Binary a => Binary (Partitions a)

-- | RDD aka Resilient Distributed Data
class Serializable b => RDD a b where

    -- | Execute the context and get the partitions
    exec :: Context -> a b -> IO (Partitions b)

    rddDictS :: a b -> Static (SerializableDict [b])
    rddDictS = undefined

    rddDict :: a b -> SerializableDict [b]
    rddDict = undefined
                            
    flow :: Context -> a b -> Process (Blocks b)
    flow = undefined

data RDDDict a b where
    RDDDict :: forall a b . RDD a b => RDDDict a b

-- | Split the data into number of partitions
-- Ensure that number of partitions are limited to maximum number of
-- elements in the list. Also that an empty list is partitioned into
-- an empty list
splits :: Int -> [a] -> [[a]]
splits n _ | n <= 0 = error "Splits must be non-negative"
splits _ [] = []
splits n xs = splits' ms xs []
    where
      l = length xs
      ms = case l `divMod` n of
             (0,rm) -> replicate rm 1
             (m,0)  -> replicate n  m
             (m,r)  -> replicate r (m+1) ++ replicate (n-r) m 
      splits' :: [Int] -> [a] -> [[a]] -> [[a]]
      splits' _ [] rs = reverse rs
      splits' [] _ rs = reverse rs
      splits' (m:ms) xs rs = splits' ms qs (ps:rs)
          where (ps,qs) = splitAt m xs


