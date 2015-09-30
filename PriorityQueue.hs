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
import Data.IORef (IORef, newIORef, readIORef, writeIORef, modifyIORef')
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
relativeIndex idx offset = (offset + idx) `mod` bitWidth

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
    let (idx,deficit') = calcIdxAndDeficit
    offidx <- getOffIdx idx
    push offidx ent { deficit = deficit' }
    updateBits idx
  where
    calcIdxAndDeficit = (idx,deficit')
      where
        !total = deficitTable ! weight ent + deficit ent
        !deficit' = total `mod` deficitSteps
        !idx      = total `div` deficitSteps
    getOffIdx idx = do
        offset <- readIORef offsetRef
        let !offidx = relativeIndex idx offset
        return offidx
    push offidx ent' =
        updateArray anchors offidx $ \q -> ((), RTQ.enqueue ent' q)
    updateBits idx = modifyIORef' bitsRef $ \bits -> setBit bits idx

-- | Dequeuing an entry. Queue is updated.
dequeue :: Queue a -> IO (Entry a)
dequeue Queue{..} = do
    !idx <- getIdx
    !offidx <- getOffIdx idx
    ent <- updateArray anchors offidx RTQ.dequeue
    updateOffset offidx
    checkEmpty offidx >>= updateBits idx
    return ent
  where
    getIdx = firstBitSet <$> readIORef bitsRef
    getOffIdx idx = relativeIndex idx <$> readIORef offsetRef
    checkEmpty offidx = RTQ.null <$> readArray anchors offidx
    updateOffset offset' = writeIORef offsetRef offset'
    updateBits idx isEmpty = modifyIORef' bitsRef shiftClear
      where
        shiftClear bits
          | isEmpty   = clearBit (shiftR bits idx) 0
          | otherwise = shiftR bits idx

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
