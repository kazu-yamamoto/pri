{-# LANGUAGE BangPatterns #-}

module RealTimeQueue (
    RealTimeQueue
  , empty
  , RealTimeQueue.null
  , enqueue
  , dequeue
  ) where

----------------------------------------------------------------

newtype LazyList a = Lazy [a]
newtype StrictList a = Strict [a]

data RealTimeQueue a = RealTimeQueue  (LazyList a)    -- front
               !(StrictList a)  -- rear
                (LazyList a)    -- pointer copy to front

----------------------------------------------------------------

conS :: a -> StrictList a -> StrictList a
conS x (Strict xs) = Strict xs'
  where
    !xs' = x:xs

rotate :: LazyList a -> StrictList a -> LazyList a -> LazyList a
rotate (Lazy [])     (Strict [y])    (Lazy zs) = Lazy (y:zs)
rotate (Lazy (x:xs)) (Strict (y:ys)) (Lazy zs) = Lazy (x:rs)
  where
    Lazy rs = rotate (Lazy xs) (Strict ys) (Lazy (y:zs)) -- reverse
rotate _ _ _ = error "rotate"

exec :: RealTimeQueue a -> RealTimeQueue a
exec (RealTimeQueue f r (Lazy (_:x))) = RealTimeQueue f r (Lazy x) -- forcing
exec (RealTimeQueue f r (Lazy  []))   = RealTimeQueue f' (Strict []) f'
  where
    f' = rotate f r (Lazy []) -- fixme

----------------------------------------------------------------

null :: RealTimeQueue a -> Bool
null (RealTimeQueue (Lazy []) (Strict []) (Lazy [])) = True
null _                                               = False

empty :: RealTimeQueue a
empty = RealTimeQueue (Lazy []) (Strict []) (Lazy [])

enqueue :: a -> RealTimeQueue a -> RealTimeQueue a
enqueue x (RealTimeQueue f r s) = let !z = exec (RealTimeQueue f r' s) in z
  where
    r' = conS x r -- fixme

dequeue :: RealTimeQueue a -> (a, RealTimeQueue a)
dequeue (RealTimeQueue (Lazy (x:f)) r s) = (x,q)
  where
    !q = exec (RealTimeQueue (Lazy f) r s)
dequeue _                                = error "dequeue"
