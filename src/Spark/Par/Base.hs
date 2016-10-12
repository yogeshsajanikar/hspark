module Spark.Par.Base where

import Control.Distributed.Process 

data Fetch a = Fetch ProcessId SendPort
             | Terminate

data Packet a = Packet ProcessId a
              | EmptyPacket ProcessId

fetch :: a -> Process (Packet a)
fetch x = do
  f <- expect
  case f of
    Fetch pid sport -> sendChan x
    Terminate -> return EmptyPacket
