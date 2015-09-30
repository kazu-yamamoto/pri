{-# LANGUAGE ForeignFunctionInterface #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE FlexibleContexts #-}

-- Haskell implementation of H2O's priority queue.
-- https://github.com/h2o/h2o/blob/master/lib/http2/scheduler.c

module PriorityQueue where

import Data.Array (Array, listArray, (!))
import Data.Array.IO (IOArray, newArray, readArray, writeArray)
import Data.Bits (setBit, clearBit, shiftR)
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
foreign import ccall unsafe "strings.h ffsll"
    c_ffs :: CLLong -> CLLong

-- | Finding first bit set. O(1)
--
-- >>> firstBitSet $ setBit 0 63
-- 63
-- >>> firstBitSet $ setBit 0 62
-- 62
-- >>> firstBitSet $ setBit 0 1
-- 1
-- >>> firstBitSet $ setBit 0 0
-- 0
firstBitSet :: Word64 -> Int
firstBitSet x = ffs x - 1
  where
    ffs = fromIntegral . c_ffs . fromIntegral

----------------------------------------------------------------

new :: IO (Queue a)
new = Queue <$> newIORef 0 <*> newIORef 0 <*> newArray (0, bitWidth - 1) RTQ.empty

-- | Enqueuing an entry. Queue is updated.
enqueue :: Queue a -> Entry a -> IO ()
enqueue Queue{..} ent = do
    offset <- readIORef offsetRef
    let !total = deficitTable ! weight ent + deficit ent
        !deficit' = total `mod` deficitSteps
        !idx      = total `div` deficitSteps
        !offidx = relativeIndex offset idx
        !ent' = ent { deficit = deficit' }
    updateArray anchors offidx $ \q -> ((), RTQ.enqueue ent' q)
    updateBits idx
  where
    updateBits idx = do
        bits <- readIORef bitsRef
        let !bits' = setBit bits idx
        writeIORef bitsRef bits'

-- | Dequeuing an entry. Queue is updated.
dequeue :: Queue a -> IO (Entry a)
dequeue Queue{..} = do
    bits <- readIORef bitsRef
    offset <- readIORef offsetRef
    let !idx = firstBitSet bits
        !offidx = relativeIndex offset idx
    ent <- updateArray anchors offidx RTQ.dequeue
    updateOffset offidx
    checkEmpty offidx >>= updateBits bits idx
    return ent
  where
    checkEmpty offidx = RTQ.null <$> readArray anchors offidx
    updateOffset offset' = writeIORef offsetRef offset'
    updateBits bits idx isEmpty = writeIORef bitsRef bits''
      where
        !bits' = shiftR bits idx
        !bits'' = if isEmpty then clearBit bits' 0 else bits'

updateArray :: IOArray Int a -> Int -> (a -> (b,a)) -> IO b
updateArray arr idx f = do
    x <- readArray arr idx
    let (r,x') = f x
    writeArray arr idx x'
    return r

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
