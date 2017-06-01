{-# LANGUAGE DataKinds #-}
{-# LANGUAGE TemplateHaskell #-}

import Database.LMDB
import System.Directory ( createDirectoryIfMissing )

database [d|

    refA = xxx :: DBRef Bool

    singleB = xxx :: Single 'BoundedKey Int String

    multiC = xxx :: Multi 'BoundedKey Int String

    |]


noisefile :: FilePath
noisefile = "test1.noise"

dbpath :: FilePath
dbpath = "test1.db"

main = do
    currentTime >>= writeFile noisefile . show
    createDirectoryIfMissing True dbpath
    env <- openDBEnv dbpath (Just noisefile)
    putStrLn $ "opened "++ dbpath
    e <- runDB env $ do
            dbtrace "started transaction"
            initSingle singleB
            dbtrace "finished initSingle"
            initMulti multiC
            writeDBRef refA True
            store singleB 5 "foo"
            insert multiC 6 "bar"
    print e
    closeDBEnv env
