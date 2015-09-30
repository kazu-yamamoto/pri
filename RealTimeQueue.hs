{-# LANGUAGE BangPatterns #-}

module RealTimeQueue (
    empty
  , enqueue
  , dequeue
  ) where

----------------------------------------------------------------

newtype LazyList a = Lazy [a]
newtype StrictList a = Strict [a]

data RQ a = RQ  (LazyList a)    -- front
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

exec :: RQ a -> RQ a
exec (RQ f r (Lazy (_:x))) = RQ f r (Lazy x)      -- forcing
exec (RQ f r (Lazy  []))   = RQ f' (Strict []) f'
  where
    f' = rotate f r (Lazy []) -- fixme

----------------------------------------------------------------

empty :: RQ a
empty = RQ (Lazy []) (Strict []) (Lazy [])

enqueue :: a -> RQ a -> RQ a
enqueue x (RQ f r s) = let !z = exec (RQ f r' s) in z
  where
    r' = conS x r -- fixme

dequeue :: RQ a -> (a, RQ a)
dequeue (RQ (Lazy (x:f)) r s) = (x,q)
  where
    !q = exec (RQ (Lazy f) r s)
dequeue _                     = error "dequeue"
