{-# LANGUAGE ForeignFunctionInterface #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE BangPatterns #-}

-- https://en.wikipedia.org/wiki/Find_first_set
-- https://github.com/h2o/h2o/blob/master/lib/http2/scheduler.c

module H2O where

import Control.Monad (when)
import Data.Array (Array, listArray, (!))
import Data.Array.IO (IOArray, newArray, readArray, writeArray)
import Data.Bits (setBit, clearBit, shiftL)
import Data.IORef (IORef, newIORef, readIORef, writeIORef)
import Data.Sequence (Seq, (<|), ViewR(..))
import qualified Data.Sequence as S
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
    bitsRef   :: IORef Word64
  , offsetRef :: IORef Int
  , anchors   :: IOArray Int (Seq (Entry a))
  }

----------------------------------------------------------------

bitWidth :: Int
bitWidth = 64

deficitSteps :: Int
deficitSteps = 65536

----------------------------------------------------------------

offsetList :: [Int]
offsetList = map calc idxs ++ [0]
  where
    idxs :: [Double]
    idxs = [1..256]
    calc x = round (2**(8 - logBase 2 x) * 16128)

offsetTable :: Array Int Int
offsetTable = listArray (0,256) offsetList

----------------------------------------------------------------

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
new = Queue <$> newIORef 0 <*> newIORef 0 <*> newArray (0, bitWidth - 1) S.empty

enqueue :: Queue a -> Entry a -> IO ()
enqueue Queue{..} Entry{..} = do
    bits <- readIORef bitsRef
    offset <- readIORef offsetRef
    let !off' = offsetTable ! (weight - 1) + deficit
        !deficit' = off' `mod` deficitSteps
        !off      = off' `div` deficitSteps
        !ent = Entry weight deficit' item
        !n = bitWidth - 1 - off
        !bits' = setBit bits n
        !idx = (offset + off) `mod` bitWidth
    writeIORef bitsRef bits'
    q <- readArray anchors idx
    let q' = ent <| q
    writeArray anchors idx q'

dequeue :: Queue a -> IO (Entry a)
dequeue Queue{..} = do
    bits <- readIORef bitsRef
    offset <- readIORef offsetRef
    let !zeroes = countLeadingZero64 bits
        !bits' = shiftL bits zeroes
    writeIORef bitsRef bits'
    let !off = (offset + zeroes) `mod` bitWidth
    writeIORef offsetRef off
    q <- readArray anchors off
    let q' :> ent = S.viewr q
    writeArray anchors off q'
    when (S.null q') $ do
        let !bits'' = clearBit bits' (bitWidth - 1)
        writeIORef bitsRef bits''
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
