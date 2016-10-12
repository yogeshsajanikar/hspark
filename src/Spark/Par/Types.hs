{-# LANGUAGE DataKinds, TypeOperators, PolyKinds #-}
{-# LANGUAGE InstanceSigs #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE KindSignatures #-}
module Spark.Par.Types where

import Control.Distributed.Process
import Control.Monad
import Control.Applicative
import Data.Functor
import Spark.DSL

-- | An IOVar equivalent of 
data NIVar a = NIVar ProcessId

new :: NodePar (NIVar (Block a))
new = NodePar $ New

put :: NIVar a -> a -> NodePar (NIVar a)
put = NodePar $ Put 

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


toNodePar :: RDD a -> NodePar a
toNodePar = undefined

