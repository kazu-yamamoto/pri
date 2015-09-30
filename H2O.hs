{-# LANGUAGE ForeignFunctionInterface #-}
{-# LANGUAGE RecordWildCards #-}

-- https://en.wikipedia.org/wiki/Find_first_set
-- https://github.com/h2o/h2o/blob/master/lib/http2/scheduler.c

module H2O where

import Data.Word
import Control.Monad
import Data.Array
import Data.Array.IO
import Data.Bits
import Data.IORef
import Data.Sequence as S
import Foreign.C.Types

foreign import ccall unsafe "strings.h flsll"
    c_fls :: CLong -> CLong

findFirstBitSet :: Word64 -> Int
findFirstBitSet = fromIntegral . c_fls . fromIntegral

type Weight = Int

data Entry a = Entry {
    weight :: Int
  , deficit :: Int
  , item :: a
  } deriving Show

data Queue a = Queue {
    bitsRef   :: IORef Word64
  , offsetRef :: IORef Int
  , anchors   :: IOArray Int (Seq (Entry a))
  }

offsetList :: [Int]
offsetList = map calc idxs ++ [0]
  where
    idxs :: [Double]
    idxs = [1..256]
    calc x = round $ (2**(8 - logBase 2 x) * 16128)

offsetTable :: Array Int Int
offsetTable = listArray (0,256) offsetList

new :: IO (Queue a)
new = Queue <$> newIORef 0 <*> newIORef 0 <*> newArray (0,63) empty

enqueue :: Entry a -> Queue a -> IO ()
enqueue Entry{..} Queue{..} = do
    bits <- readIORef bitsRef
    offset <- readIORef offsetRef
    let off' = offsetTable ! (weight - 1) + deficit
        deficit' = off' `mod` 65536
        off      = off' `div` 65536
        ent = Entry weight deficit' item
        n = 8 * 8 - 1 - off
        bits' = setBit bits n
        idx = (offset + off) `div` 64
    writeIORef bitsRef bits'
    q <- readArray anchors idx
    writeArray anchors idx $ ent <| q

dequeue :: Queue a -> IO (Entry a)
dequeue Queue{..} = do
    bits <- readIORef bitsRef
    offset <- readIORef offsetRef
    let zeroes = 63 - findFirstBitSet bits + 1 -- fixme
        bits' = shiftL bits zeroes
    writeIORef bitsRef bits'
    let off = (offset + zeroes) `mod` 64
    writeIORef offsetRef off
    q <- readArray anchors off
    let q' :> ent = viewr q
    writeArray anchors off q'
    when (S.null q') $ do
        writeIORef bitsRef $ clearBit bits' 63
    return ent

go :: IO ()
go = do
    q <- new :: IO (Queue String)
    enqueue (Entry 201 0 "a") q
    enqueue (Entry 101 0 "b") q
    enqueue (Entry 1 0 "c") q
    dequeue q >>= print
    dequeue q >>= print
    dequeue q >>= print
