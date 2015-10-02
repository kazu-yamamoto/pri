{-# LANGUAGE BangPatterns #-}

module PriorityQueue2 where

import Data.Array (Array, listArray, (!))
import Data.Heap (Heap)
import qualified Data.Heap as H

----------------------------------------------------------------

type Weight = Int
type Priority = Int

data Ent a = Ent a Weight Priority deriving Show

instance Eq (Ent a) where
    Ent _ _ p1 == Ent _ _ p2 = p1 == p2

instance Ord (Ent a) where
    Ent _ _ p1 < Ent _ _ p2 = p1 < p2
    Ent _ _ p1 <= Ent _ _ p2 = p1 <= p2

data PriorityQueue a = PriorityQueue Int (Heap (Ent a))

----------------------------------------------------------------

magicPriority :: Priority
magicPriority = 0

prioritySteps :: Int
prioritySteps = 65536

priorityList :: [Int]
priorityList = map calc idxs
  where
    idxs = [1..256] :: [Double]
    calc w = round (fromIntegral prioritySteps / w)

priorityTable :: Array Int Int
priorityTable = listArray (1,256) priorityList

weightToPriority :: Weight -> Priority
weightToPriority w = priorityTable ! w

----------------------------------------------------------------

newEntry :: a -> Weight -> Ent a
newEntry x w = Ent x w magicPriority

isNewEntry :: Ent a -> Bool
isNewEntry (Ent _ _ p) = p == magicPriority

----------------------------------------------------------------

newQ :: PriorityQueue a
newQ = PriorityQueue 0 H.empty

enqueue :: PriorityQueue a -> Ent a -> PriorityQueue a
enqueue (PriorityQueue base heap) ent@(Ent x w p) = PriorityQueue base heap'
  where
    !b = if isNewEntry ent then base else p
    !p' = b + weightToPriority w
    !ent' = Ent x w p'
    !heap' = H.insert ent' heap

dequeue :: PriorityQueue a -> (Ent a, PriorityQueue a)
dequeue (PriorityQueue _ heap) = (ent, PriorityQueue base' heap')
  where
    Just (ent@(Ent _ _ p), heap') = H.uncons heap
    !base' = p `mod` prioritySteps

----------------------------------------------------------------

main :: IO ()
main = do
    let q0 = newQ
        q1 = enqueue q0 (newEntry "a" 201)
        q2 = enqueue q1 (newEntry "b" 101)
        q3 = enqueue q2 (newEntry "c"   1)
    go 1000 q3

go :: Int -> PriorityQueue String -> IO ()
go  0 _ = return ()
go !n q = do
    let (x,q') = dequeue q
    print x
    let q'' = enqueue q' x
    go (n - 1) q''

