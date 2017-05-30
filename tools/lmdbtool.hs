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
import Data.List (sortBy)
import Data.Ord (comparing)
import Data.Int
import Data.Bits
import Data.Binary
import Foreign.Ptr
import Foreign.Storable
import qualified Data.ByteString.Internal          as S
import Foreign.Marshal.Alloc
import Foreign.ForeignPtr
import Control.Exception
import System.Directory

aliases = [ ("listTables","list")
          , ("createTable","create")
          , ("deleteTable","drop")
          , ("clearTable","clear")
          , ("insertKey","insert")
          , ("deleteKey","delete")
          , ("lookupVal","lookup")
          , ("keysOf","keys")
          , ("valsOf","vals")
          ]

formatAssocList width indent as = h width (B.length indent) indent as
    where
        h _ _ str [] = str
        h max tot str ((x,y):ys) = if l x y ys + tot > max then let str' = B.concat [str,"\n",indent,b x y ys]
                                                                    in h max 0 str' ys
                                                           else let str' = B.concat [str,b x y ys]
                                                                    in h max (tot + l x y ys) str' ys
        b x y xs = B.concat [x," => ",y,if null xs then "\n" else ", "]
        l x y xs = B.length (b x y xs)

unaliasArgs args | length args < 2 = return args
unaliasArgs args@(a:b:args') = do
    bPath <- doesDirectoryExist (B.unpack a)
    if bPath then return (a:f b:args')
             else return args
    where f x = case lookup x aliases of
                    Just y  -> y
                    Nothing -> x

usage :: IO ()
usage = let cs = 
               [ ["    ","list"]
               , ["    ","create",tbl]
               , ["    ","drop  ",tbl]
               , ["    ","clear ",tbl]
               , ["{*} ","insert",atbl,key,val]
               , ["{*} ","delete",atbl,key]
               , ["{*} ","keys  ",atbl]
               , ["{*} ","vals  ",atbl]
               , ["{*} ","lookup",atbl,key]
               -- , ["{*} ",path,"toList   ",atbl]
                 -- Variation of toList
               , ["    ","show  ",mtbl]
               ]
            ds=[ -- Copy commands
                 ["{+} copyTable",bool,path,tbl,path,mtbl]
               ]
            path = "<path>"
            tbl = "<table>"
            mtbl = "[<table>]"
            atbl = "(@|<table>)"
            key = "<key>"
            val = "<value>"
            bool = "[true|false]"
            fmt xs = " " <> B.unwords xs
            in do
                putStrLn "Usage: lmdbtool [-p] <path> <Infix-Command>"
                putStrLn "       lmdbtool [-p] copyTable (true|false) <path> <table> <path> [<table>]"
                putStrLn ""
                putStrLn "  -p  Accept final parameter on stdin"
                putStrLn ""
                putStrLn "Infix Commands:" >> mapM_ (B.putStrLn . fmt) cs
                putStrLn ""
                putStrLn "Prefix Command:" >> mapM_ (B.putStrLn . fmt) ds
                putStrLn ""
                putStrLn "Notes:  {*} These commands accept ‘@’ as name of Main table (unnamedDB)."
                putStrLn "            To match library, ‘lookupVal’ is accepted as an alias for ‘lookup’."
                putStrLn ""
                putStrLn "        {+} On copy commands, ‘true’ indicates to allow duplicate keys."
                putStrLn "            To copy a single key, pipe the output of ‘lookup’ into ‘insert’."
                putStrLn ""
                putStrLn "Aliases:"
                B.putStrLn (formatAssocList 80 "        " (sortBy (comparing fst) aliases))

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
    args' <- if take 1 args == ["-p"] then drop 1 . (args ++) . (:[]) <$> B.getLine
                                      else return args
    
    args'' <- unaliasArgs args'
    case args'' of
        [path,"list"]               -> mapM_ B.putStrLn =<< listTables (u path)
        [path,"create",tbl]         -> v $ createTable (u path) tbl
        [path,"drop",tbl]           -> v $ deleteTable (u path) tbl
        [path,"clear",tbl]          -> clearTable (u path) tbl
        [path,"insert","@",key,val] -> v $ insertKeyUnnamed (u path) key val
        [path,"insert",tbl,key,val] -> v $ insertKey (u path) tbl key val
        [path,"delete","@",key]     -> v $ deleteKeyUnnamed (u path) key
        [path,"delete",tbl,key]     -> v $ deleteKey (u path) tbl key
        [path,"keys","@"]         -> mapM_ B.putStrLn =<< keysOfUnnamed (u path)
        [path,"keys",tbl]         -> mapM_ (putKey (u path) tbl) =<< keysOf (u path) tbl
        [path,"vals","@"]         -> mapM_ B.putStrLn =<< valsOfUnnamed (u path)
        [path,"vals",tbl]         -> mapM_ (putVal (u path) tbl) =<< valsOf (u path) tbl
        [path,"lookup","@",key]  -> B.putStrLn =<< fromMaybe "" <$> lookupValUnnamed (u path) key
        [path,"lookup",tbl,key]  -> B.putStrLn =<< fromMaybe "" <$> lookupVal (u path) tbl key
        -- [path,"toList","@"]         -> print =<< toListUnnamed (u path)
        -- [path,"toList",tbl]         -> print =<< toList (u path) tbl
        -- Variation of toList
        [path,"show",tbl]           -> mapM_ putPair =<< toList (u path) tbl
        [path,"show"]               -> mapM_ (putTbl path) =<< listTables (u path)
        -- Copy commands
        ["copyTable","true",path,tbl,dest]      -> v $ copyTable True (u path) tbl (u dest) tbl
        ["copyTable","false",path,tbl,dest]     -> v $ copyTable False (u path) tbl (u dest) tbl
        ["copyTable","true",path,tbl,dest,tbl2] -> v $ copyTable True (u path) tbl (u dest) tbl2
        ["copyTable","false",path,tbl,dest,tbl2]-> v $ copyTable False (u path) tbl (u dest) tbl2
        _ -> usage

lookupValUnnamed x k = withDBSDo x $ \dbs -> do
    d <- unnamedDB dbs
    mb <- unsafeFetch d k
    case mb of
        Just (val,final) -> do
            force val `seq` final
            return (Just val)
        Nothing -> return Nothing

insertKeyUnnamed x k v = withDBSCreateIfMissing x $ \dbs -> do
    d <- unnamedDB dbs
    add d k v
deleteKeyUnnamed x k = withDBSCreateIfMissing x $ \dbs -> do
    d <- unnamedDB dbs
    delete d k 

keysOfUnnamed x = withDBSDo x $ \dbs -> do
    d <- unnamedDB dbs
    (keysVals,final) <- unsafeDumpToList d
    let keys = map (B.copy . fst) keysVals
    force keys `seq` final 
    return keys

valsOfUnnamed x = withDBSDo x $ \dbs -> do
    d <- unnamedDB dbs
    (keysVals,final) <- unsafeDumpToList d
    let vals = map (B.copy . snd) keysVals
    force vals `seq` final 
    return vals

toListUnnamed x = withDBSDo x $ \dbs -> do
    d <- unnamedDB dbs
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
    d <- unnamedDB dbs
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
    d <- unnamedDB dbs
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
