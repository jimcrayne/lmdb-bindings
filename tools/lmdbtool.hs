{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE NoMonomorphismRestriction #-}
import Database.LMDB

import System.Environment
import qualified Data.ByteString.Char8 as B
import Data.Maybe
import Data.Monoid
import Text.Printf
import Control.Applicative
import Control.Monad


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
        ["keysOf",path,tbl]         -> mapM_ B.putStrLn =<< keysOf (u path) tbl
        ["valsOf",path,tbl]         -> mapM_ B.putStrLn =<< valsOf (u path) tbl
        ["lookupVal",path,tbl,key]  -> B.putStrLn =<< fromMaybe "" <$> lookupVal (u path) tbl key
        ["toList",path,tbl]         -> print =<< toList (u path) tbl
        -- Variation of toList
        ["show",path,tbl]           -> mapM_ putPair =<< toList (u path) tbl
        ["show",path]               -> mapM_ (putTbl path) =<< listTables (u path)
        _ -> usage
