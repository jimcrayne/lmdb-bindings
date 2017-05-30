{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE OverloadedStrings #-}

-- import Test.QuickCheck
import Database.LMDB.Raw
import Database.LMDB
import Control.Monad
import Control.Concurrent.MVar
import Control.Concurrent
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
    createAppendDupDB env "table1"

withSystemTempDirectory0 _ f = f "DBFOLDER"

initGlobalEnv = withSystemTempDirectory0 "checkLMDBXXXX" $ \dir -> do
    printf "lmdb environment: %s\n" dir
    createDirectoryIfMissing True dir
    b <- doesDirectoryExist dir
    if b then do
                
                putStrLn "dir exists." 
                writeFile (dir </> "A" ) "TEMPR" 
                -- xs <- listDirectory dir
                -- print xs
                --pid <- runCommand ("caja " ++ dir)
                --exitC <- waitForProcess pid
                --printf "exitC=%s\n" (show exitC)
                getPermissions dir >>= print
                p <- canonicalizePath dir
                print p
         else putStrLn "NO DIRECTORY!"
    initDBS dir >>= putMVar globalDBS
    createTable1

withGlobal = bracket (readMVar globalDBS ) shutDownDBS 
withTable1 = bracket (readMVar globalDBS >>= flip openAppendDupDB "table1") closeDB 

-- | 'prop_always' always succeeds, just testing my quickcheck understanding
-- prop_always = forAll (arbitrary :: Gen Int) (const True)


-- runTests = $quickCheckAll
main = do
    initGlobalEnv 
    getLine
    -- void runTests 
    dbs <- readMVar globalDBS
    withTable1 $ \db -> do
        add db "key" "This"
        add db "key" "should"
        add db "key" "still"
        add db "key" "be"
        add db "key" "in"
        add db "key" "order."
    putStrLn "Environments:"
    listEnv >>= print
    putStr " 1 isOpen : " >> isOpenEnv "DBFOLDER" >>= print
    shutDownDBS dbs
    putStrLn "Closing dbs ..."
    threadDelay 2000
    putStr " 2 isOpen : " >> isOpenEnv "DBFOLDER" >>= print
    newdbs <- openDBS dbs
    modifyMVar_ globalDBS (const . return $ newdbs)
    withTable1 $ \db -> do
        add db "key" "And"
        add db "key" "this"
        add db "key" "follows."
    withTable1 $ \db -> do
        (kvs,finalize) <- unsafeDumpToList db
        mapM_ (print . snd) kvs
        finalize
    shutDownDBS dbs
    getLine
    
    
        
