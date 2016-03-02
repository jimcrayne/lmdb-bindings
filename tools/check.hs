{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE OverloadedStrings #-}

import Test.QuickCheck
import Database.LMDB.Raw
import Database.LMDB
import Control.Monad
import Control.Concurrent.MVar
import Control.Exception (bracket)
import System.IO.Unsafe

import System.Directory -- (createDirectoryIfMissing, doesDirectoryExist,listDirectory)
import System.IO.Temp
import System.FilePath
import Text.Printf
import Data.IORef
import qualified  Data.ByteString.Char8 as B

import System.Process

{-# NOINLINE globalDBS #-}
globalDBS :: MVar DBS
globalDBS = unsafePerformIO $ newEmptyMVar

createTable1 = do
    env <- readMVar globalDBS
    putStrLn "Creating table1 with DUPSORT flag."
    createDupDB env "table1"

withSystemTempDirectory0 _ f = f "DBFOLDER"

initGlobalEnv = withSystemTempDirectory0 "checkLMDBXXXX" $ \dir -> do
    printf "lmdb environment: %s\n" dir
    createDirectoryIfMissing True dir
    b <- doesDirectoryExist dir
    if b then do
                
                putStrLn "dir exists." 
                writeFile (dir </> "A" ) "TEMPR" 
                xs <- listDirectory dir
                print xs
                pid <- runCommand ("caja " ++ dir)
                exitC <- waitForProcess pid
                printf "exitC=%s\n" (show exitC)
                getPermissions dir >>= print
                p <- canonicalizePath dir
                print p
         else putStrLn "NO DIRECTORY!"
    initDBS dir >>= putMVar globalDBS
    createTable1

withGlobal = bracket (readMVar globalDBS ) shutDownDBS 
withTable1 = bracket (readMVar globalDBS >>= flip openDupDB "table1") closeDB 

-- | 'prop_always' always succeeds, just testing my quickcheck understanding
prop_always = forAll (arbitrary :: Gen Int) (const True)


runTests = $quickCheckAll
main = do
    initGlobalEnv 
    getLine
    void runTests 
    dbs <- readMVar globalDBS
    withTable1 $ \db -> do
        store db "key" "This"
        store db "key" "should"
        store db "key" "still"
        store db "key" "be"
        store db "key" "in"
        store db "key" "order."
    withTable1 $ \db -> do
        store db "key" "And"
        store db "key" "this"
        store db "key" "follows."
    withTable1 $ \db -> do
        (kvs,finalize) <- unsafeDumpToList db
        mapM_ (print . snd) kvs
        finalize
    shutDownDBS dbs
    getLine
    
    
        
