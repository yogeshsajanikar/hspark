-- |
-- Module : Spark.Static
--
-- Each block is defined by a block of data residing in a process. We
-- spawn the process on remote nodes, and send the data. Similarly a
-- mapping stage defines a process that fetches the data from
-- dependent process and holds it till asked by master or another
-- process. 
module Spark.Static where

import Control.Distributed.Process
import Control.Distributed.Process.Serializable
import Control.Distributed.Static
import Spark.Block

