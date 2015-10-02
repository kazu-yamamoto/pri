{-# LANGUAGE ForeignFunctionInterface #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE FlexibleContexts #-}

-- Haskell implementation of H2O's priority queue.
-- https://github.com/h2o/h2o/blob/master/lib/http2/scheduler.c

module PriorityQueue where

import Control.Concurrent.STM
import Control.Monad (replicateM)
import Data.Array (Array, listArray, (!))
import Data.Array.MArray (newListArray, readArray)
import Data.Bits (setBit, clearBit, shiftR)
import Data.Word (Word64)
import Foreign.C.Types (CLLong(..))

----------------------------------------------------------------

type Weight = Int

data Entry a = Entry {
    weight :: Int
  , deficit :: Int
  , item :: a
  } deriving Show

data Queue a = Queue {
    bitsRef   :: TVar Word64
  , offsetRef :: TVar Int
  , anchors   :: TArray Int (TQueue (Entry a))
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

new :: STM (Queue a)
new = Queue <$> newTVar 0 <*> newTVar 0 <*> newAnchors
  where
    newAnchors = replicateM bitWidth newTQueue
                 >>= newListArray (0, bitWidth - 1)

-- | Enqueuing an entry. Queue is updated.
enqueue :: Queue a -> Entry a -> STM ()
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
        offset <- readTVar offsetRef
        let !offidx = relativeIndex idx offset
        return offidx
    push offidx ent' = readArray anchors offidx >>= flip writeTQueue ent'
    updateBits idx = modifyTVar' bitsRef $ flip setBit idx

-- | Dequeuing an entry. Queue is updated.
dequeue :: Queue a -> STM (Entry a)
dequeue Queue{..} = do
    !idx <- getIdx
    !offidx <- getOffIdx idx
    ent <- pop offidx
    updateOffset offidx
    checkEmpty offidx >>= updateBits idx
    return ent
  where
    getIdx = firstBitSet <$> readTVar bitsRef
    getOffIdx idx = relativeIndex idx <$> readTVar offsetRef
    pop offidx = readArray anchors offidx >>= readTQueue
    checkEmpty offidx = readArray anchors offidx >>= isEmptyTQueue
    updateOffset offset' = writeTVar offsetRef offset'
    updateBits idx isEmpty = modifyTVar' bitsRef shiftClear
      where
        shiftClear bits
          | isEmpty   = clearBit (shiftR bits idx) 0
          | otherwise = shiftR bits idx

----------------------------------------------------------------

main :: IO ()
main = do
    q <- atomically new :: IO (Queue String)
    atomically $ enqueue q (Entry 201 0 "a")
    atomically $ enqueue q (Entry 101 0 "b")
    atomically $ enqueue q (Entry   1 0 "c")
    go 1000 q

go :: Int -> Queue String -> IO ()
go  0 _ = return ()
go !n q = do
    x <- atomically $ dequeue q
    print x
    atomically $ enqueue q x
    go (n - 1) q
