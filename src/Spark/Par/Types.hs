{-# LANGUAGE DataKinds, TypeOperators, PolyKinds #-}
{-# LANGUAGE InstanceSigs #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE KindSignatures #-}
module Spark.Par.Types where

import Control.Distributed.Process
import Control.Monad
import Control.Applicative
import Data.Functor
import Data.IORef
import Data.Typeable

newtype Block a = Block [Process [a]]

data RDD :: * -> * where
  DataRDD :: Block a -> RDD a
  MapRDD :: Closure (a -> b) -> RDD a -> RDD b
  FilterRDD :: Closure (a -> Bool) -> RDD a -> RDD a
  ReduceRDD :: Ord k => Closure (k -> v -> v -> u) -> RDD (k, v) -> RDD (k, u)


-- | Convert a closure into a list of 
initRDD :: Typeable a => [Closure [a]] -> RDD a
initRDD = DataRDD . Block . map unClosure
    


-- | An IOVar equivalent of 
data NIVar a = NIVar (IORef (NIVarContents a))

data NIVarContents a = Full ProcessId
                     | Blocked [Block a -> Plan]

new :: NodePar (NIVar (Block a))
new = NodePar $ New

put :: NIVar a -> a -> NodePar (NIVar a)
put = undefined

get :: NIVar a -> NodePar a
get = undefined


data Plan :: * where
  Fork :: Plan -> Plan -> Plan
  Done :: Plan
  Get  :: NIVar (Block a) -> (Block a -> Plan) -> Plan
  Put  :: NIVar (Block a) -> Block a -> Plan
  New  :: (NIVar (Block a) -> Plan) -> Plan

-- Note that we cannot convert this plan into usual Functor,
-- Applicative or Monad as the definition of closure is not conducive
-- to define the instance of these type classes

-- Nevertheless we can still use the idea of continuation monad to
-- create a continuation for the plan

-- | Node parallel monad for running tasks on set of nodes
newtype NodePar a = NodePar { runNodePar :: (a -> Plan) -> Plan }


instance Functor NodePar where

  fmap :: (a -> b) -> NodePar a -> NodePar b
  f `fmap` n = NodePar $ \c -> runNodePar n (c . f)

instance Applicative NodePar where

  (<*>) = ap

  pure a = NodePar $ \c -> c a


instance Monad NodePar where

  return = pure

  m >>= k = NodePar $ \c -> runNodePar m $ \a -> runNodePar (k a) c


toNodePar :: RDD a -> NodePar (Block a)
toNodePar (DataRDD b) = return b
toNodePar (MapRDD f b) = do
  x <- toNodePar b
  undefined
toNodePar _ = undefined


runPar :: NodePar (Block a) -> Process a
runPar = undefined
