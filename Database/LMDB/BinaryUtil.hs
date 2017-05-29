module Database.LMDB.BinaryUtil where

import Data.Binary
import Data.Binary.Get
import Data.Binary.Put
import qualified Data.ByteString as S
import qualified Data.ByteString.Lazy as L
import Data.Maybe

-- | Given a reference key value, search sequence of encoded key/length/value
-- triples until a matching key is found.  Returns the parsed value when there
-- is a match, and 'Nothing' otherwise.
findForKey :: (Eq k, Binary k, Binary v) => k -> Get (Maybe v)
findForKey k = do
    nil <- isEmpty
    if nil
     then return Nothing
     else do
        storedk <- get
        vlen <- getWord32le
        if storedk /= k
            then do skip (fromIntegral vlen)
                    done <- isEmpty
                    if not done then findForKey k
                                else return Nothing
            else Just <$> get

-- | Like 'findForKey' except that it does not stop on the first match and
-- instead returns all matches as a list.
findManyForKey :: (Eq k, Binary k, Binary v) => k -> Get [v]
findManyForKey k = do
    m <- findForKey k
    fromMaybe (return []) $ do
        x <- m
        Just $ do
            xs <- findManyForKey k
            return (x:xs)

-- | This is similar to 'getManyChunks' except that it does not preserve the
-- unparsed 'L.ByteStream' form of the objects.
getMany :: Binary a => Get [a]
getMany = do
    done <- isEmpty
    if done then return []
            else do
                x <- get
                xs <- getMany
                return (x:xs)

-- | This 'Get' can be used to deserialize as many instances of a type that can
-- fit in a given 'L.ByteString'.  Each returned pair is the parsed and
-- unparsed ('L.ByteString') form of the same object.  Note that no lengths are
-- encoded and it relies on the object's 'Binary' instance to consume only a
-- limited amount of bytes during the parse.
getManyChunks :: Binary a => Get [(a,L.ByteString)]
getManyChunks = do
    done <- isEmpty
    if done then return []
            else do
                (x,vlen) <- lookAhead $ do
                    i0 <- bytesRead
                    obj <- get
                    i1 <- bytesRead
                    return (obj, i1 - i0)
                bs <- getLazyByteString (fromIntegral vlen)
                xbs <- getManyChunks
                return $ (x,bs):xbs

-- | returns a list of (key,value) pairs with the values still encoded as
-- bytestrings, but the keys decoded.
--
-- It is assumed that 32-bit lengths precede the encoded values so that they
-- can be skipped without parsing them.
--
-- Unlike 'getManyChunks', the 'Binary' instance for the values is not used and
-- is free to consume all available input.
splitKeyChunks :: Binary k => k -> Get [(k,L.ByteString)]
splitKeyChunks _ = do
    done <- isEmpty
    if done then return []
            else do
        key <- get
        vlen <- getWord32le
        {-
        (key,vlen) <- lookAhead $ do
            i0 <- bytesRead
            k <- get
            vlen <- getWord32le
            i1 <- bytesRead
            return (k, fromIntegral vlen) --  + i1 - i0)
        -}
        bs <- getLazyByteString (fromIntegral vlen)
        xs <- splitKeyChunks (error "dummy value")
        return $ (key,bs) : xs



-- | Append a serialized key, a 32-bit byte-count for a serialized value, and a
-- serialized value to the end of a given 'L.ByteString'.
--
-- As of this writing, this function is only used in the case of 'HashedKey' so
-- as to preserve the original unhashed key value within the database.  The
-- byte-count is a convenience for skipping hash collisions without the
-- possibly-expensive deserializion of the colliding value.
appendKeyValue :: (Binary k, Binary v) => L.ByteString -> k -> v -> L.ByteString
appendKeyValue bs k v = bs `L.append` kv
 where
    kv = runPut $ do
            put k
            putWord32le vlen
            putLazyByteString bv
    bv = runPut $ put v
    vlen = fromIntegral $ L.length bv


-- | Like 'decode' but accepts a strict 'S.ByteString' as input.
decodeStrict :: Binary a => S.ByteString -> a
decodeStrict = decode . L.fromChunks . (:[])


