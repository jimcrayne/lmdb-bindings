{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE NoMonomorphismRestriction #-}
import Database.LMDB

import System.Exit
import System.Environment
import qualified Data.ByteString.Char8 as B
import qualified Data.ByteString.Lazy as L
import Data.Maybe
import Data.Monoid
import Text.Printf
import Control.Applicative
import Control.Monad
import Control.DeepSeq (force)
import System.Process
import Database.LMDB.Flags
import Data.Word
import Data.Int
import Data.Bits
import Data.Binary
import Foreign.Ptr
import Foreign.Storable
import qualified Data.ByteString.Internal          as S
import Foreign.Marshal.Alloc
import Foreign.ForeignPtr

usage :: IO ()
usage = let cs = 
               [ ["listTables",path]         
               , ["createTable",path,tbl]    
               , ["deleteTable",path,tbl]    
               , ["clearTable",path,tbl]     
               , ["insertKey",path,tbl,key,val]  
               , ["deleteKey",path,tbl,key]  
               , ["keysOf",path,tbl]         
               , ["valsOf",path,tbl]         
               , ["lookupKey",path,tbl,key]  
               , ["toList",path,tbl]         
                 -- Variation of toList
               , ["show",path,tbl]
               , ["show",path]                  ]
            path = "<path>"
            tbl = "<table>"
            key = "<key>"
            val = "<value>"
            fmt xs = "    " <> B.unwords xs
            in do
                putStrLn "Usage: ldmbtool <command> [<options>]\n"
                putStrLn "Commands:" >> mapM_ (B.putStrLn . fmt) cs

main :: IO ()
main = do
    args <- map B.pack <$> getArgs
    let u = B.unpack
        v = void
        putPair (x,y) = B.putStrLn (x <> ": " <> y)
        putPairI n (x,y) = B.putStrLn (B.replicate n ' ' <> x <> ": " <> y)
        putTbl path tbl = do 
            putStrLn "---"
            B.putStr tbl >> putStrLn ":"
            mapM_ (putPairI 2) =<< toList (u path) tbl
    case args of
        ["listTables",path]         -> mapM_ B.putStrLn =<< listTables (u path)
        ["createTable",path,tbl]    -> v $ createTable (u path) tbl
        ["deleteTable",path,tbl]    -> v $ deleteTable (u path) tbl
        ["clearTable",path,tbl]     -> clearTable (u path) tbl
        ["insertKey",path,tbl,key,val] -> v $ insertKey (u path) tbl key val
        ["deleteKey",path,tbl,key]  -> v $ deleteKey (u path) tbl key
        ["keysOf",path,"@"]         -> mapM_ B.putStrLn =<< keysOfUnnamed (u path)
        ["keysOf",path,tbl]         -> mapM_ (putKey (u path) tbl) =<< keysOf (u path) tbl
        ["valsOf",path,"@"]         -> mapM_ B.putStrLn =<< valsOfUnnamed (u path)
        ["valsOf",path,tbl]         -> mapM_ (putVal (u path) tbl) =<< valsOf (u path) tbl
        ["lookupVal",path,tbl,key]  -> B.putStrLn =<< fromMaybe "" <$> lookupVal (u path) tbl key
        ["toList",path,"@"]         -> print =<< toListUnnamed (u path)
        ["toList",path,tbl]         -> print =<< toList (u path) tbl
        -- Variation of toList
        ["show",path,tbl]           -> mapM_ putPair =<< toList (u path) tbl
        ["show",path]               -> mapM_ (putTbl path) =<< listTables (u path)
        _ -> usage


keysOfUnnamed x = withDBSDo x $ \dbs -> do
    d <- internalUnamedDB dbs
    (keysVals,final) <- unsafeDumpToList d
    let keys = map (B.copy . fst) keysVals
    force keys `seq` final 
    return keys

valsOfUnnamed x = withDBSDo x $ \dbs -> do
    d <- internalUnamedDB dbs
    (keysVals,final) <- unsafeDumpToList d
    let vals = map (B.copy . snd) keysVals
    force vals `seq` final 
    return vals

toListUnnamed x = withDBSDo x $ \dbs -> do
    d <- internalUnamedDB dbs
    (xs,final) <- unsafeDumpToList d
    let ys   = map copy xs
        copy (x,y) = (B.copy x, B.copy y)
    force ys `seq` final 
    return ys

printInt64 bstr = let x :: Int64
                      x = decode (L.fromChunks [bstr])
                      in print x

printWord64 bstr = 
                  let x :: Word64
                      x = decode (L.fromChunks [bstr])
                      in print x

printInt32 bstr = let x :: Int32
                      x = decode (L.fromChunks [bstr])
                      in print x

printWord32 bstr =let x :: Word32
                      x = decode (L.fromChunks [bstr])
                      in print x

printLengthPrefixed bstr = 
                let x :: B.ByteString
                    x = decode (L.fromChunks [bstr])
                    in B.putStrLn x

putVal x tbl = \str -> withDBSDo x $ \dbs -> do
    d <- internalUnamedDB dbs
    mb <- unsafeFetch d (tbl <> "FLAGS")
    let f :: B.ByteString -> Word64
        f x = decode (L.fromChunks [x])
    case mb of
        Nothing -> B.putStrLn str
        Just (x,fin) -> do
            {-let (fptr,_,_) = S.toForeignPtr x 
                in withForeignPtr fptr $ \ptr -> do
                    flags <- peek (castPtr ptr)-}
                    let flags = decode (L.fromChunks [x])
                    case () of
                       _ | (flags `has` flagValInt64) -> fin >> printInt64 str
                       _ | (flags `has` flagValInt32) -> fin >> printInt32 str
                       _ | (flags `has` flagValWord64) -> fin >> printWord64 str
                       _ | (flags `has` flagValWord32) -> fin >> printWord32 str
                       _ | (flags `has` flagValLengthPrefixed) -> fin >> printLengthPrefixed str
                       _ | (flags `has` flagValBinary) -> fin >> printBinary str

putKey x tbl = \str -> withDBSDo x $ \dbs -> do
    d <- internalUnamedDB dbs
    mb <- unsafeFetch d (tbl <> "FLAGS")
    let f :: B.ByteString -> Word64
        f x = decode (L.fromChunks [x])
    case mb of
        Nothing -> B.putStrLn str
        Just (x,fin) -> do
            {-let (fptr,_,_) = S.toForeignPtr x 
                in withForeignPtr fptr $ \ptr -> do
                    flags <- peek (castPtr ptr)-}
                    let flags = decode (L.fromChunks [x])
                    case () of
                       _ | (flags `has` flagKeyInt64) -> fin >> printInt64 str
                       _ | (flags `has` flagKeyInt32) -> fin >> printInt32 str
                       _ | (flags `has` flagKeyWord64) -> fin >> printWord64 str
                       _ | (flags `has` flagKeyWord32) -> fin >> printWord32 str
                       _ | (flags `has` flagKeyLengthPrefixed) -> fin >> printLengthPrefixed str
                       _ | (flags `has` flagKeyBinary) -> fin >> printBinary str


printBinary s = do
    xxdResult <- readCreateProcessWithExitCode (shell "xxd") (B.unpack s)
    case xxdResult of
        (ExitSuccess,out,"") -> putStrLn out
        _ -> putStrLn "(ERROR: Is xxd installed?)"
