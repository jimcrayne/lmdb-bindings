{-# LANGUAGE AutoDeriveTypeable #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE CPP #-}
module Database.LMDB
    ( DBS
    -- * Public DBS Interface
    , withDBSDo
    , withDBSCreateIfMissing
    , initDBS
    , openDBS
    , shutDownDBS 
    , createDB
    , openDB
    , createDupsortDB
    , createDupDB
    , openDupsortDB
    , openDupDB
    , closeDB
    , lengthDB
    , dropDB
    , delete
    , store
    -- * Internal and/or Unsafe Functions (DBS Interface)
    , unsafeFetch
    , internalUnamedDB
    , unsafeDumpToList
    , unsafeDumpToListOp
    -- * Query for open environment(s) (Process Globals)
    , isOpenEnv
    , listEnv
    -- * Atomic functions using paths, and names only
    , listTables
    , listTablesCreateIfMissing
    , deleteTable
    , createTable
    , clearTable
    , insertKey
    , deleteKey
    , lookupVal
    , toList
    , keysOf
    , valsOf
    -- * Like the atomic functions, but leaves environment open 
    , listTables'
    , deleteTable'
    , createTable'
    , clearTable'
    , insertKey'
    , deleteKey'
    , lookupVal'
    , toList'
    , keysOf'
    , valsOf'
    ) where

import System.FilePath
import System.Directory
import Control.Concurrent (myThreadId, ThreadId(..))
import Database.LMDB.Raw
import Data.ByteString.Internal
import qualified Data.ByteString.Char8 as S
import Foreign.ForeignPtr hiding (addForeignPtrFinalizer) 
import Foreign.Concurrent
import Foreign.Ptr
import Data.Word
import Foreign.C.Types
import Control.Exception
import System.DiskSpace
import Foreign.Storable
import Control.Monad
import Control.Applicative
import Control.Monad.Loops
import Foreign.Marshal.Alloc
import Data.IORef
import Control.Concurrent.MVar
import Data.Global
import qualified Data.HashTable.IO as H
import Data.String
import Data.Typeable
import qualified Data.HashTable.Class as Class
import qualified Data.HashTable.ST.Basic as Basic
-- import Control.Monad.Primitive
import Control.Monad.Extra
import Control.DeepSeq (force)

dputStrLn :: String -> IO ()
#ifdef DEBUG
dputStrLn str = putStrLn ("(DEBUG) " ++ str)
#else
dputStrLn str = return ()
#endif

newtype HTable s k v = HT (Basic.HashTable s k v) deriving Typeable

type HashTable k v = H.IOHashTable HTable k v

deriving instance Class.HashTable (HTable)


-- | DBS
--
-- The acronym 'DBS' stands for Database System.  The idea is we have a generic
-- handle for initializing and closing down your database system regardless of
-- whether it is actually MySQL or LDMB or whatever. In the language of LMDB,
-- this is called an "environment", and this type can be thought of as a handle
-- to your "LMDB Database Environment".
data DBS = DBS { dbEnv :: MDB_env
               , dbDir :: FilePath
               , dbsIsOpen :: MVar Bool
               } -- deriving Typeable

-- | DBHandle
--
-- Unlike DBS, this is a handle to one specific database within the environment.
-- Here, "Database" means a simple key-value store, and nothing more grand.
--
-- This is an opaque type, but internally it contains a reference to an environment
-- so functions dealing with databases do not need to pass an environment handle 
-- (DBS type) as an explicit parameter.
data DBHandle = DBH { dbhEnv :: (MDB_env, MVar Bool)
                    , dbhDBI :: MDB_dbi 
                    , compiledWriteFlags :: MDB_WriteFlags
                    , dbhIsOpen :: MVar Bool
                    }

-- | withDBSDo 
--
--      dir - directory containing data.mdb, or an
--            existing empty directory if creating
--            a new environment
--
--      action - function which takes a DBS and preforms
--               various operations on it's contained 
--               databases.
--
-- Shorthand for @bracket (initDBS dir) shutDownDBS action@
withDBSDo :: FilePath -> (DBS -> IO a) -> IO a
withDBSDo dir action = bracket (initDBS dir) shutDownDBS action


withDBSCreateIfMissing dir action = do
    createDirectoryIfMissing True dir
    withDBSDo dir action

foreign import ccall getPageSizeKV :: CULong

isOpenEnv dir = do
    h <- H.new :: IO (HashTable S.ByteString DBS)
    let registryMVar = declareMVar "LMDB Environments" h
    registry <- readMVar registryMVar 
    result <- H.lookup registry dir 
    case result of
        Nothing -> return False
        Just (DBS _ _ mvar) -> readMVar mvar

listEnv = do
    h <- H.new :: IO (HashTable S.ByteString DBS)
    let registryMVar = declareMVar "LMDB Environments" h
    registry <- readMVar registryMVar 
    es <- H.toList registry 
    (stillOpen,closed) <- partitionM (readMVar . dbsIsOpen . snd) es
    mapM_ (H.delete registry . fst) closed
    return $ map fst stillOpen

-- | initDBS
--
--      dir - directory containing data.mdb, or an
--            existing empty directory if creating
--            a new environment
--
-- Start up and initialize whatever engine(s) provide access to the 
-- environment specified by the given path. 
initDBS :: FilePath -> IO DBS
initDBS dir = do
    h <- H.new :: IO (HashTable S.ByteString DBS)
    let registryMVar = declareMVar "LMDB Environments" h
    registry <- takeMVar registryMVar -- lock registry
    mbAlreadyOpen <- H.lookup registry (fromString dir)
    case mbAlreadyOpen of
        Just dbs@(DBS env _ mvar) -> do
            isOpen <- readMVar mvar
            if isOpen then do 
                        putMVar registryMVar registry -- unlock registry
                        return dbs
                      else do
                        (env',_) <- newEnv dir (Just mvar)
                        putMVar registryMVar registry -- unlock registry
                        return (dbs {dbEnv = env' })
        Nothing -> do
            (env',mvar') <- newEnv dir Nothing
            let retv = DBS env' dir mvar'
            H.insert registry (fromString dir) retv
            putMVar registryMVar registry -- unlock registry
            return retv
    where
        newEnv dir maybeMVar = do
            tid <- myThreadId
            env <- mdb_env_create 

            -- I tried to ask for pagesize, but it segfaults
            -- stat <- mdb_env_stat env 
            -- putStrLn "InitDBS after stat"

            -- mapsize can be very large, but should be a multiple of pagesize
            -- the haskell bindings take an Int, so I just use maxBound::Int
            space <- getAvailSpace dir
            let pagesize :: Int
                pagesize = fromIntegral $ getPageSizeKV
                mapsize1  = maxBound - rem maxBound pagesize
                mapsize2  = fromIntegral $ space - rem (space - (500*1024)) (fromIntegral pagesize)
                mapsize  = min mapsize1 mapsize2
            mdb_env_set_mapsize env mapsize

            -- maxreaders, I chose this arbitrarily.
            -- The vcache package doesn't set it, so I'll comment it out for now
            mdb_env_set_maxreaders env 10000 

            -- LDMB is designed to support a small handful of databases.
            -- i choose 10 (arbitarily) as maxdbs
            -- The vcache package uses 5
            mdb_env_set_maxdbs env 10 

            mdb_env_open env dir [{-todo?(options)-}]
            case maybeMVar of
                Nothing -> do
                    isopen <- newMVar True
                    return (env, isopen)
                Just mvar -> do
                    modifyMVar_ mvar (const $ return True) 
                    return (env, mvar)


-- | openDBS - shouldn't really need this
--
--  NOT RECOMMENDED. This is a no op in the 
--  case the environment is open, otherwise
--  it calls 'initDBS'. Normally use 'withDBS'
--  or 'initDBS' and don't shut down until program
--  terminates.
openDBS :: DBS -> IO DBS
openDBS x@(DBS env dir isopenmvar) = do
    isopen <- readMVar isopenmvar
    if (not isopen) 
        then do
            putStrLn ("OPENING dbs: " ++ dir)
            initDBS dir
        else return x


-- | shutDownDBS environment 
--
-- Shutdown whatever engines were previously started in order
-- to access the set of databases represented by 'environment'.
shutDownDBS :: DBS -> IO ()
shutDownDBS (DBS env dir emvar)= do
    open <- takeMVar emvar
    if open 
        then do
            --modifyMVar_ emvar (return . const False)
            mdb_env_close env
            putMVar emvar False
        else putMVar emvar open

internalOpenDB :: [MDB_DbFlag] -> DBS -> ByteString -> IO DBHandle
internalOpenDB flags (DBS env dir eMvar) name = do 
    e <- readMVar eMvar
    if e 
       then bracket (mdb_txn_begin env Nothing False)
                    (mdb_txn_commit)
                    $ \txn -> do
                             db <- mdb_dbi_open txn (Just (S.unpack name)) flags
                             isopen <- newMVar True
                             return $ DBH (env,eMvar) db (compileWriteFlags []) isopen
       else isShutdown 
    where isShutdown = error ("Cannot open '" ++ S.unpack name ++ "' due to environment being shutdown.") 

internalUnamedDB ::  DBS -> IO DBHandle
internalUnamedDB (DBS env dir eMvar) = do 
    e <- readMVar eMvar
    if e 
       then bracket (mdb_txn_begin env Nothing False)
                    (mdb_txn_commit)
                    $ \txn -> do
                             db <- mdb_dbi_open txn Nothing flags
                             isopen <- newMVar True
                             return $ DBH (env,eMvar) db (compileWriteFlags []) isopen
       else isShutdown 
    where isShutdown = error ("Cannot open unamed db due to environment being shutdown.") 
          flags = []

-- createDB db name
-- Opens the specified named database, creating one if it does not exist.
createDB ::  DBS -> ByteString -> IO DBHandle
createDB = internalOpenDB [MDB_CREATE]

-- createDupsortDB db name
-- Like 'createDB' but the database is flagged DUPSORT
createDupsortDB ::  DBS -> ByteString -> IO DBHandle
createDupsortDB = internalOpenDB [MDB_CREATE, MDB_DUPSORT]

-- createDupDB db name
-- Like 'createDupsortDB' but the data comparison function is set to always return 1
createDupDB ::  DBS -> ByteString -> IO DBHandle
createDupDB dbs name = do
    handle@(DBH (env,mvar0) dbi flags mvar1) <- internalOpenDB [MDB_CREATE, MDB_DUPSORT] dbs name
    compare <- wrapCmpFn (\_ _ -> return 1)
    bracketIfOpen mvar0 (mdb_txn_begin env Nothing False)
            mdb_txn_commit
            (\txn -> mdb_set_dupsort txn dbi compare)
    return handle

bracketIfOpen mvar init close action = do
    b <- readMVar mvar
    if b then (bracket init close action)
         else (error "bracketIfOpen")

-- | openDB db name
-- Open a named database.
openDB ::  DBS -> ByteString -> IO DBHandle
openDB = internalOpenDB []

-- | openDupsortDB db name
-- Like 'openDB' but DUPSORT specified.
openDupsortDB ::  DBS -> ByteString -> IO DBHandle
openDupsortDB = internalOpenDB [MDB_DUPSORT]

-- openDupDB db name
-- Like 'openDupsortDB' but the data comparison function is set to always return 1
openDupDB ::  DBS -> ByteString -> IO DBHandle
openDupDB dbs name = do
    handle@(DBH (env,mvar0) dbi flags mvar1) <- internalOpenDB [MDB_DUPSORT] dbs name
    compare <- wrapCmpFn (\_ _ -> return 1)
    bracketIfOpen mvar0 (mdb_txn_begin env Nothing False)
            mdb_txn_commit
            (\txn -> mdb_set_dupsort txn dbi compare)
    return handle

-- | closeDB db name
-- Close the database.
closeDB :: DBHandle -> IO ()
closeDB (DBH (env,eMvar) dbi writeflags mvar) = do
    eOpen <- readMVar eMvar
    if eOpen
      then do
        dbisopen <- readMVar mvar
        when (dbisopen) $ do
            modifyMVar_ mvar (return . const False)
            mdb_dbi_close env dbi
      else error ("Cannot close database due to environment being already shutdown.") 


-- | unsafeFetch dbhandle key - lookup a key in the database
--
--      dbhandle - Database in which to perform lookup
--      key      - key to look up
--
-- IF the key is not found, then return Nothing, otherwise
-- return Just (value,finalizer) where:
--
--      value     - ByteString backed by ForiegnPtr whose finalizer
--                  commits the transaction
--      finalizer - IO action which commits the transaction. Call it only if
--                  you are sure the ByteString is no longer needed. 
--                  NOTE: GHC will commit the transaction anyway, so it isn't 
--                  necessary to ever call this at all.
--
-- Warning: The ByteString is backed by a ForeignPtr which will become
-- invalid when the transaction is committed. Be sure it is not accessed
-- in any of the following 3 scenarios:
--
--      1) the environment is closed  (after call to 'shutDownDBS')
--      2) the database is closed (after call to 'closeDB')
--      3) the provided finalizer was called
--
unsafeFetch :: DBHandle -> ByteString -> IO (Maybe (ByteString, IO()) )
unsafeFetch (DBH (env,eMvar) dbi _ mvar) key = do
    txn <- mdb_txn_begin env Nothing False
    let (fornptr,offs,len) = toForeignPtr key
    mabVal <- withForeignPtr fornptr $ \ptr -> 
                mdb_get txn dbi $ MDB_val (fromIntegral len) (ptr `plusPtr` offs)
    case mabVal of
        Nothing -> do
                mdb_txn_commit txn
                return Nothing
        Just (MDB_val size ptr)  -> do
            fptr <- newForeignPtr_ ptr
            let commitTransaction = do dputStrLn ("Finalizing (fetch " ++ S.unpack key ++")")
                                       oEnv <- readMVar eMvar
                                       when oEnv $ do
                                           oDB <- readMVar mvar
                                           when oDB $ mdb_txn_commit txn
            addForeignPtrFinalizer fptr commitTransaction 
            return . Just $ (fromForeignPtr fptr 0  (fromIntegral size), finalizeForeignPtr fptr)

-- | lengthDB db - returns the number of (key,value) entries in provided database.
lengthDB (DBH (env,_) dbi writeflags mvar) =  -- todo check mvars
    bracket 
            (mdb_txn_begin env Nothing False)
            (mdb_txn_commit)
            $ \txn -> fmap ms_entries $ mdb_stat txn dbi

-- | drop db - delete a database
dropDB (DBH (env,_) dbi writeflags mvar) =  -- todo check mvars
    bracket 
            (mdb_txn_begin env Nothing False)
            (mdb_txn_commit)
            $ \txn -> mdb_drop txn dbi

-- | delete db key 
--
-- Delete the key in the database. Return False if 
-- the key is not found.
delete :: DBHandle -> ByteString -> IO Bool
delete (DBH (env,mv0) dbi writeflags mvar) key = do --todo check environment mvar
    bracketIfOpen mv0
            (mdb_txn_begin env Nothing False)
            (mdb_txn_commit)
            $ \txn -> do
                let (fornptr,offs,len) = toForeignPtr key
                mabVal <- withForeignPtr fornptr $ \ptr -> do
                    let key' = MDB_val (fromIntegral len) (ptr `plusPtr` offs)
                    mdb_get txn dbi key'
                maybe (return False) 
                      (\val -> do
                                let (fornptr',offs',len') = toForeignPtr key
                                withForeignPtr fornptr $ \ptr' ->
                                    let key' = MDB_val (fromIntegral len') (ptr' `plusPtr` offs')
                                        in mdb_del txn dbi key' (Just val))
                      mabVal

-- | store db key value
-- Store a key-value pair in the database. Returns False
-- if the key already existed.
store :: DBHandle -> ByteString -> ByteString -> IO Bool
store (DBH (env,mv0) dbi writeflags _) key val =  -- todo check mvars
    bracketIfOpen mv0
            (mdb_txn_begin env Nothing False)
            (mdb_txn_commit) $ \txn ->
        let (kp,koff,klen) = toForeignPtr key
            (vp,voff,vlen) = toForeignPtr val
            in withForeignPtr kp $ \kptr -> withForeignPtr vp $ \vptr -> do
                let key' = MDB_val (fromIntegral klen) (kptr `plusPtr` koff)
                    val' = MDB_val (fromIntegral vlen) (vptr `plusPtr` voff)
                mdb_put writeflags txn dbi key' val'

-- | unsafeDumpToList 
--
--      db - Database
--    
-- Returns (kvs,finalize):
--
--      kvs - a lazy association list of ByteStrings with the (key,value)-
--            pairs from `db`
--
--      finalize - action which commits the transaction. Call it only if
--                 you are sure the ByteStrings are no longer needed. 
--                 NOTE: GHC will commit the transaction anyway, so it isn't 
--                 necessary to ever call this at all.
--
-- Ordinarily, the transaction remains open until all ForeignPtr's are
-- finalized. If you are concerned that an open transaction is a waste
-- of resources and that GHC will not finalize the strings promptly,
-- then you may use the provided finalizer to close the transaction
-- immediately.
--
-- Warning: ByteStrings are backed by ForeignPtr's which become invalid when
-- the transaction is committed. Be sure they are not accessed in any of the
-- following 3 scenarios:
--
--      1) the environment is closed 
--      2) the database is closed
--      3) the provided finalizer was called
--
unsafeDumpToList :: DBHandle -> IO ([(ByteString, ByteString)],IO ())
unsafeDumpToList dbs  = unsafeDumpToListOp MDB_NEXT dbs

unsafeDumpToListOp :: MDB_cursor_op -> DBHandle -> IO ([(ByteString, ByteString)],IO ())
unsafeDumpToListOp flag (DBH (env,emvar) dbi _ mvar) = do
    txn <- mdb_txn_begin env Nothing False
    cursor <- mdb_cursor_open txn dbi
    ref <- newMVar (0::Int)
    let finalizer = do
            modifyMVar_ ref (return . subtract 1) -- (\x -> (x,x-1)) 
            r <- readMVar ref
            dputStrLn ("Finalizing #" ++ show r)
            when (r == 0) $ do
                oEnv <- readMVar emvar
                when oEnv $ do
                   oDB <- readMVar mvar
                   when oDB $ mdb_txn_commit txn
        finalizeAll = do
           dputStrLn "finalizeAll"
           modifyMVar_ ref (return . const (-1)) 
           oEnv <- readMVar emvar
           oDB <- readMVar mvar
           when (not oDB || not oEnv) $ 
            error "ERROR finalizer returned from unsafeDumpToList was manually called after closeDB/shutDownDBS."
           mdb_txn_commit txn
    xs <- unfoldWhileM (\(_,b) -> b) $ alloca $ \pkey -> alloca $ \pval -> do
        bFound <- mdb_cursor_get flag cursor pkey pval 
        if bFound 
            then do
                MDB_val klen kp <- peek pkey
                MDB_val vlen vp <- peek pval
                fkp <- newForeignPtr_ kp
                fvp <- newForeignPtr_ vp
                modifyMVar_ ref (return . (+2))
                addForeignPtrFinalizer fkp finalizer
                addForeignPtrFinalizer fvp finalizer
                return ([ (fromForeignPtr fkp 0 (fromIntegral klen)
                         ,fromForeignPtr fvp 0 (fromIntegral vlen) ) ], bFound)
            else return ([], bFound)
    mdb_cursor_close cursor
    return $ (concatMap fst xs,finalizeAll)

listTables x = withDBSDo x $ \dbs -> do
    db <- internalUnamedDB dbs
    (keysVals,final) <- unsafeDumpToListOp MDB_NEXT_NODUP db
    let keys = map (S.copy . fst) keysVals
    force keys `seq` final 
    return keys

listTables' dbs = bracket (internalUnamedDB dbs) closeDB $ \db -> do
    (keysVals,final) <- unsafeDumpToListOp MDB_NEXT_NODUP db
    let keys = map (S.copy . fst) keysVals
    force keys `seq` final 
    return keys

listTablesCreateIfMissing x = withDBSCreateIfMissing x $ \dbs -> do
    db <- internalUnamedDB dbs
    (keysVals,final) <- unsafeDumpToListOp MDB_NEXT_NODUP db
    let keys = map (S.copy . fst) keysVals
    force keys `seq` final 
    return keys

deleteTable x n = withDBSDo x $ \dbs -> do
    DBH (env,_) dbi _ mvar <- openDB dbs n
    bracket (mdb_txn_begin env Nothing False)
            mdb_txn_commit
            (\txn -> mdb_drop txn dbi)

deleteTable' dbs n = bracket (openDB dbs n) closeDB $ \(DBH (env,_) dbi _ mvar) -> 
    bracket (mdb_txn_begin env Nothing False)
            mdb_txn_commit
            (\txn -> mdb_drop txn dbi)
    
createTable x n = withDBSCreateIfMissing x $ \dbs -> createDB dbs n
createTable' dbs n = createDB dbs n

clearTable x n = withDBSDo x $ \dbs -> do
    DBH (env,_) dbi _ mvar <- openDB dbs n
    bracket (mdb_txn_begin env Nothing False)
            mdb_txn_commit
            (\txn -> mdb_clear txn dbi)

clearTable' dbs n = bracket (openDB dbs n) closeDB $ \(DBH (env,_) dbi _ mvar) -> 
    bracket (mdb_txn_begin env Nothing False)
            mdb_txn_commit
            (\txn -> mdb_clear txn dbi)

insertKey x n k v = withDBSCreateIfMissing x $ \dbs -> do
    d <- createDB dbs n
    store d k v

insertKey' dbs n k v = bracket (createDB dbs n) closeDB $ \d -> store d k v

deleteKey x n k = withDBSDo x $ \dbs -> do
    d <- openDB dbs n
    delete d k 

deleteKey' dbs n k = bracket (openDB dbs n) closeDB $ \d -> delete d k

lookupVal x n k = withDBSDo x $ \dbs -> do
    d <- openDB dbs n
    mb <- unsafeFetch d k
    case mb of
        Just (val,final) -> do
            force val `seq` final
            return (Just val)
        Nothing -> return Nothing

lookupVal' dbs n k = bracket (openDB dbs n) closeDB $ \d -> do
    mb <- unsafeFetch d k
    case mb of
        Just (val,final) -> do
            force val `seq` final
            return (Just val)
        Nothing -> return Nothing

toList x n = withDBSDo x $ \dbs -> do
    d <- openDB dbs n
    (xs,final) <- unsafeDumpToList d
    let ys   = map copy xs
        copy (x,y) = (S.copy x, S.copy y)
    force ys `seq` final 
    return ys

toList' dbs n = bracket (openDB dbs n) closeDB $ \d -> do
    (xs,final) <- unsafeDumpToList d
    let ys   = map copy xs
        copy (x,y) = (S.copy x, S.copy y)
    force ys `seq` final 
    return ys

keysOf x n = withDBSDo x $ \dbs -> do
    d <- openDB dbs n
    (keysVals,final) <- unsafeDumpToList d
    let keys = map (S.copy . fst) keysVals
    force keys `seq` final 
    return keys

keysOf' dbs n = bracket (openDB dbs n) closeDB $ \d -> do
    (keysVals,final) <- unsafeDumpToList d
    let keys = map (S.copy . fst) keysVals
    force keys `seq` final 
    return keys

valsOf x n = withDBSDo x $ \dbs -> do
    d <- openDB dbs n
    (keysVals,final) <- unsafeDumpToList d
    let vals = map (S.copy . snd) keysVals
    force vals `seq` final 
    return vals

valsOf' dbs n = bracket (openDB dbs n) closeDB $ \d -> do
    (keysVals,final) <- unsafeDumpToList d
    let vals = map (S.copy . snd) keysVals
    force vals `seq` final 
    return vals
