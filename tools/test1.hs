{-# LANGUAGE DataKinds #-}
{-# LANGUAGE TemplateHaskell #-}

import Control.Concurrent
import Database.LMDB
import System.Directory ( createDirectoryIfMissing )

database [d|

    refA = xxx :: DBRef Bool

    singleB = xxx :: Single 'BoundedKey Int String

    multiC = xxx :: Multi 'BoundedKey Int String

    fam = xxx :: String -> Single 'BoundedKey Int Int

    |]


noisefile :: FilePath
noisefile = "test1.noise"

dbpath :: FilePath
dbpath = "test1.db"

main = do
    currentTime >>= writeFile noisefile . show
    createDirectoryIfMissing True dbpath
    env <- openDBEnv (error "todo DBSpec") dbpath (Just noisefile)
    putStrLn $ "opened "++ dbpath
    forkIO $ do
        e <- runDB env $ do
                {-
                dbtrace "started transaction"
                initSingle singleB
                dbtrace "finished initSingle"
                initMulti multiC
                -}
                writeDBRef refA True
                store singleB 5 "foo"
                insert multiC 6 "bar"
                dblift $ threadDelay 1000000 -- delay transaction commit for 1 second
        print ("ready to use dbi", e)

    threadDelay 250000 -- 1/4 second
    e <- runDB env (dbtrace "started next" *> fetch singleB 5)
    print e
    closeDBEnv env
