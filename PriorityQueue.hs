{-# LANGUAGE ForeignFunctionInterface #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE BangPatterns #-}

-- Haskell implementation of H2O's priority queue.
-- https://github.com/h2o/h2o/blob/master/lib/http2/scheduler.c

module PriorityQueue where

import Data.Array (Array, listArray, (!))
import Data.Array.IO (IOArray, newArray, readArray, writeArray)
import Data.Bits (setBit, clearBit, shiftL)
import Data.IORef (IORef, newIORef, readIORef, writeIORef)
import Data.Word (Word64)
import Foreign.C.Types (CLLong(..))

import RealTimeQueue (RealTimeQueue)
import qualified RealTimeQueue as RTQ

----------------------------------------------------------------

type Weight = Int

data Entry a = Entry {
    weight :: Int
  , deficit :: Int
  , item :: a
  } deriving Show

data Queue a = Queue {
    bitsRef   :: IORef Word64
  , offsetRef :: IORef Int
  , anchors   :: IOArray Int (RealTimeQueue (Entry a))
  }

----------------------------------------------------------------

bitWidth :: Int
bitWidth = 64

relativeIndex :: Int -> Int -> Int
relativeIndex offset idx = (offset + idx) `mod` bitWidth

----------------------------------------------------------------

deficitSteps :: Int
deficitSteps = 65536

deficitList :: [Int]
deficitList = map calc idxs
  where
    idxs :: [Double]
    idxs = [1..256]
    calc x = round (2**(8 - logBase 2 x) * 16128)

deficitTable :: Array Int Int
deficitTable = listArray (1,256) deficitList

----------------------------------------------------------------

-- https://en.wikipedia.org/wiki/Find_first_set
foreign import ccall unsafe "strings.h flsll"
    c_fls :: CLLong -> CLLong

-- | Counting leading zeros for 64bit words. O(1)
--
-- >>> countLeadingZero64 $ setBit 0 63
-- 0
-- >>> countLeadingZero64 $ setBit 0 62
-- 1
-- >>> countLeadingZero64 $ setBit 0 0
-- 63
countLeadingZero64 :: Word64 -> Int
countLeadingZero64 x = bitWidth - fromIntegral (c_fls (fromIntegral x))

----------------------------------------------------------------

new :: IO (Queue a)
new = Queue <$> newIORef 0 <*> newIORef 0 <*> newArray (0, bitWidth - 1) RTQ.empty

-- | Enqueuing an entry. Queue is updated.
enqueue :: Queue a -> Entry a -> IO ()
enqueue Queue{..} ent = do
    bits <- readIORef bitsRef
    offset <- readIORef offsetRef
    let !total = deficitTable ! weight ent + deficit ent
        !deficit' = total `mod` deficitSteps
        !idx      = total `div` deficitSteps
        !n = bitWidth - 1 - idx
        !bits' = setBit bits n
    writeIORef bitsRef bits'
    let !offidx = relativeIndex offset idx
        !ent' = ent { deficit = deficit' }
    q <- RTQ.enqueue ent' <$> readArray anchors offidx
    writeArray anchors offidx q

-- | Dequeuing an entry. Queue is updated.
dequeue :: Queue a -> IO (Entry a)
dequeue Queue{..} = do
    bits <- readIORef bitsRef
    offset <- readIORef offsetRef
    let !idx = countLeadingZero64 bits
        !offidx = relativeIndex offset idx
    (ent,q) <- RTQ.dequeue <$> readArray anchors offidx
    --
    let !offset' = offidx
        !bits' = shiftL bits idx
    writeIORef offsetRef offset'
    if RTQ.null q then do
        let !bits'' = clearBit bits' (bitWidth - 1)
        writeIORef bitsRef bits''
      else
        writeIORef bitsRef bits'
    --
    writeArray anchors offidx q
    return ent

----------------------------------------------------------------

main :: IO ()
main = do
    q <- new :: IO (Queue String)
    enqueue q (Entry 201 0 "a")
    enqueue q (Entry 101 0 "b")
    enqueue q (Entry   1 0 "c")
    go 1000 q

go :: Int -> Queue String -> IO ()
go  0 _ = return ()
go !n q = do
    x <- dequeue q
    print x
    enqueue q x
    go (n - 1) q
