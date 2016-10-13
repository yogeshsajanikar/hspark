{-# LANGUAGE NamedFieldPuns #-}
module Spark.Par.Run where

import Spark.Par.Types
import Data.IORef
import Control.Concurrent.MVar
import Control.Distributed.Process
import Control.Distributed.Process.Async
import Control.Monad
import Data.Binary hiding (put)
import Data.Typeable

-- Modified version of scheduler in monad-par, adapted for distributed
-- haskell. The main difference is that rather than forking a thread,
-- we spawn a process on a node, and await the task to be executed 

data Sched = Sched
  { no       :: NodeId,
    nodes    :: [NodeId],
    workpool :: IORef [Plan],
    idle     :: IORef [MVar Bool],
    scheds   :: [Sched] -- Global list of all per-thread workers.
  }

procSpawn :: (Typeable a, Binary a)
  => NodeId -> AsyncPar a -> Process (NIVarContents a, AsyncPar a)
procSpawn n a = 
  case a of
    AsyncPTask d p -> do
      async (AsyncRemoteTask d n p) >>= toResults
    AsyncP as _    -> toResults as
    Plain a        -> return (Full (Plain a), Plain a)

  where
    toResults as = do
      asr <- poll as
      let result = AsyncP as asr
      case asr of
        AsyncDone x  ->  return $ (Full (AsyncP as asr), result)
        AsyncPending ->  return $ (Blocked (Just a)  [], result)
        otherwise    ->  error "Process terminated?"

procResult :: (Typeable a, Binary a)
  => AsyncPar a -> Process (NIVarContents a)
procResult anc@(AsyncP as _) = do
  asr <- poll as
  case asr of
    AsyncDone x  ->  return $ Full (AsyncP as asr)
    AsyncPending ->  return $ Blocked (Just anc)  []
    otherwise    ->  error "Process terminated?"
procResult anc@(Plain a) = return (Full anc)
procresult _ = fail "error getting results"

procIResults :: (Typeable a, Binary a)
  => NIVarContents a -> Process (NIVarContents a)
procIResults Empty         = return Empty
procIResults (Full a)      = return $ Full a
procIResults (Blocked (Just a)  _)  = procResult a
procIResults b@(Blocked Nothing _)  = return b

-- | Scheduling the generated 'Plan' on set of nodes
sched :: Sched -> Plan -> Process ()
sched queue@Sched{ no }  t = loop t
  where
    loop t = case t of
      New a f -> do
        r <- liftIO $ newIORef a
        loop (f (NIVar r))

      Get (NIVar v) c -> do
        ex <- liftIO $ readIORef v
        e <- procIResults ex
        case e of
          Full a -> loop (c a)
          _other -> do
            r <- liftIO $ atomicModifyIORef v $ \e ->
              case e of
                Empty            ->   (Blocked Nothing [c],    reschedule queue)
                Full a           ->   (Full a,         loop (c a))
                Blocked async cs ->   (Blocked async (c:cs), reschedule queue)
            r
      Put (NIVar v) ax t  -> do
        (a, result)  <- procSpawn no ax
        cs <- liftIO $ atomicModifyIORef v $ \e -> case e of
               Empty            -> (a, [])
               Full _           -> error "multiple put"
               Blocked async cs -> (a, cs)

        case a of
          -- Only when the put is fulfilled, move ahead
          Full _ -> do
            mapM_ (pushWork queue. ($ result)) cs
            loop t
          _ -> loop (Put (NIVar v) result t)

      Fork child parent -> do
        pushWork queue child
        loop parent

      Done -> reschedule queue

reschedule :: Sched -> Process ()
reschedule queue@Sched { workpool } = do
  e <- liftIO $ atomicModifyIORef workpool $ \ts ->
    case ts of
      []      -> ([], Nothing)
      (t:ts') -> (ts', Just t)
  case e of
    Nothing -> steal queue
    Just t  -> sched queue t

steal :: Sched -> Process ()
steal q@Sched{ nodes, idle, scheds, no=my_no } = do
  -- printf "cpu %d stealing\n" my_no
  go scheds
  where
    go [] = do m <- liftIO $ newEmptyMVar
               r <- liftIO $ atomicModifyIORef idle $ \is -> (m:is, is)
               if length r == length nodes 
                  then do
                     -- printf "cpu %d initiating shutdown\n" my_no
                     mapM_ (\m -> liftIO $ putMVar m True) r
                  else do
                    done <- liftIO $ takeMVar m
                    if done
                       then do
                         -- printf "cpu %d shutting down\n" my_no
                         return ()
                       else do
                         -- printf "cpu %d woken up\n" my_no
                         go scheds
    go (x:xs)
      | no x == my_no = go xs
      | otherwise     = do
         r <- liftIO $ atomicModifyIORef (workpool x) $ \ ts ->
                 case ts of
                    []     -> ([], Nothing)
                    (x:xs) -> (xs, Just x)
         case r of
           Just t  -> do
              -- printf "cpu %d got work from cpu %d\n" my_no (no x)
              sched q t
           Nothing -> go xs



-- | If any worker is idle, wake one up and give it work to do.
pushWork :: Sched -> Plan -> Process ()
pushWork Sched { workpool, idle } t = do
  liftIO $ liftIO $ atomicModifyIORef workpool $ \ts -> (t:ts, ())
  idles <- liftIO $ readIORef idle
  when (not (null idles)) $ liftIO $ do
    r <- atomicModifyIORef idle (\is -> case is of
                                          [] -> ([], return ())
                                          (i:is) -> (is, putMVar i False))
    r -- wake one up

runPar :: (Binary a, Typeable a)
  => [NodeId] -> NodePar (AsyncPar a) -> Process (AsyncPar a)
runPar nodes x = do
   workpools <- replicateM (length nodes) $ (liftIO $ newIORef [])
   idle <- liftIO $ newIORef []
   let states = [ Sched { no=x, nodes=nodes, workpool=wp, idle, scheds=states }
                | (x,wp) <- zip nodes workpools ]

   m <- liftIO $ newEmptyMVar
   let master = head nodes
   forM_ (zip nodes states) $ \(node,state) ->
          if (node /= master)
             then reschedule state
             else do
                  rref <- liftIO $ newIORef Empty
                  sched state $ runNodePar (x >>= put (NIVar rref)) (const Done)
                  liftIO $ readIORef rref >>= putMVar m

   r <- liftIO $ takeMVar m
   case r of
     Full x -> return x
     _ -> error "Could not calculate"
   
