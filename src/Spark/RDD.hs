{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE DeriveGeneric #-}
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
class Typeable b => RDD a b where

    -- | Execute the context and get the partitions
    exec :: Context -> a b -> IO (Partitions b)

    flow :: ProcessId -> Context -> a b -> Process (Blocks b)
    flow = undefined


data ListRDD b = ListRDD { _listP :: Partitions b }

instance Typeable b => RDD ListRDD b where

    exec sc (ListRDD ps) = return ps

    flow master sc (ListRDD ps) = do undefined
      --return . Blocks . M.map stage master $ _dataMaps ps

-- | Create an empty RDD
emptyRDD :: Serializable a => Context -> ListRDD a
emptyRDD _ = ListRDD emptyP

-- | Create RDD from the data itself
fromListRDD :: Serializable a => Context -> Int -> [a] -> ListRDD a
fromListRDD _ n xs = ListRDD $ Partitions partitions
    where
      partitions = M.fromList ps
      ps = zip [0..] $ splits n xs

-- | RDD representing a pure map between a base with a function
data MapRDD a b c = MapRDD { _baseM :: a b
                           , _cFunM :: Closure (b -> c)
                           }

instance (RDD a b, Typeable c) => RDD (MapRDD a b) c where

    exec sc mr = case unclosure (_lookupTable sc) (_cFunM mr) of
                   Right f -> do
                     ps <- exec sc (_baseM mr)
                     return $ f <$> ps
                   Left e  -> error e

-- | Create map RDD from a function closure and base RDD
mapRDD :: (RDD a b, Serializable c) => Context -> a b -> Closure (b -> c) -> MapRDD a b c
mapRDD sc base action = MapRDD { _baseM = base, _cFunM = action }


-- | RDD representing a IO map between base RDD and a IO function
data MapRDDIO a b c = MapRDDIO { _baseI :: a b
                               , _cFunI :: Closure (b -> IO c)
                               }

-- | Create map RDD from a function closure and base RDD
mapRDDIO :: (RDD a b, Serializable c) => Context -> a b -> Closure (b -> IO c) -> MapRDDIO a b c
mapRDDIO sc base action = MapRDDIO { _baseI = base, _cFunI = action }

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


