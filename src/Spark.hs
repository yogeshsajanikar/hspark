module Spark
(
 module Spark.Context,
 module Spark.RDD,
 module Spark.MapRDD,
 module Spark.MapRDDIO,
 module Spark.ReduceRDD,
 module Spark.SeedRDD,
 module Spark.Executor,
 remoteTable
)

where

import Spark.Context
import Spark.RDD
import Spark.MapRDD hiding (__remoteTable)
import Spark.MapRDDIO hiding (__remoteTable)
import Spark.ReduceRDD hiding (__remoteTable)
import Spark.SeedRDD hiding (__remoteTable)
import Spark.Executor


import Control.Distributed.Process
import Control.Distributed.Process.Closure
import Control.Distributed.Process.Node
import Control.Distributed.Process


import qualified Spark.MapRDD as M (__remoteTable)
import qualified Spark.MapRDDIO as MI  (__remoteTable)
import qualified Spark.ReduceRDD as R (__remoteTable)
import qualified Spark.SeedRDD as S (__remoteTable)

remoteTable = M.__remoteTable
            . MI.__remoteTable
            . R.__remoteTable
            . S.__remoteTable

