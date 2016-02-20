
module Database.LMDB
    ( DBS
    , withDBSDo
    , initDBS
    , shutDownDBS 
    , createDB
    , openDB
    , closeDB
    , unsafeFetch
    , lengthDB
    , dropDB
    , delete
    , store
    , unsafeDumpToList
    ) where

import System.FilePath
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
import Control.Monad.Loops
import Foreign.Marshal.Alloc
import Data.IORef
import Control.Concurrent.MVar


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
               }

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

foreign import ccall getPageSizeKV :: CULong

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
    isopen <- newMVar True
    return (DBS env dir isopen)

-- | shutDownDBS environment 
--
-- Shutdown whatever engines were previously started in order
-- to access the set of databases represented by 'environment'.
shutDownDBS :: DBS -> IO ()
shutDownDBS (DBS env dir emvar)= do
    open <- readMVar emvar
    when open $ do
        modifyMVar_ emvar (return . const False)
        mdb_env_close env

internalOpenDB :: [MDB_DbFlag] -> DBS -> ByteString -> IO DBHandle
internalOpenDB flags (DBS env dir eMvar) name = do 
    bracket (mdb_txn_begin env Nothing False)
            (mdb_txn_commit)
            $ \txn -> do
                 eOpen <- readMVar eMvar
                 if eOpen
                  then do
                     db <- mdb_dbi_open txn (Just (S.unpack name)) flags
                     isopen <- newMVar True
                     return $ DBH (env,eMvar) db (compileWriteFlags []) isopen
                  else error ("Cannot open '" ++ S.unpack name ++ "' due to environment being shutdown.") 

-- createDB db name
-- Opens the specified named database, creating one if it does not exist.
createDB ::  DBS -> ByteString -> IO DBHandle
createDB = internalOpenDB [MDB_CREATE]

-- | openDB db name
-- Open a named database.
openDB ::  DBS -> ByteString -> IO DBHandle
openDB = internalOpenDB []

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
            let commitTransaction = do putStrLn ("(DEBUG) Finalizing (fetch " ++ S.unpack key ++")")
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
delete (DBH (env,_) dbi writeflags mvar) key = do --todo check environment mvar
    bracket 
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
store (DBH (env,_) dbi writeflags _) key val =  -- todo check mvars
    bracket 
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
unsafeDumpToList (DBH (env,emvar) dbi _ mvar) = do
    txn <- mdb_txn_begin env Nothing False
    cursor <- mdb_cursor_open txn dbi
    ref <- newMVar (0::Int)
    let finalizer = do
            modifyMVar_ ref (return . subtract 1) -- (\x -> (x,x-1)) 
            r <- readMVar ref
            putStrLn ("(DEBUG) Finalizing #" ++ show r)
            when (r == 0) $ do
                oEnv <- readMVar emvar
                when oEnv $ do
                   oDB <- readMVar mvar
                   when oDB $ mdb_txn_commit txn
        finalizeAll = do
           putStrLn "(DEBUG) finalizeAll"
           modifyMVar_ ref (return . const (-1)) 
           oEnv <- readMVar emvar
           oDB <- readMVar mvar
           when (not oDB || not oEnv) $ 
            error "ERROR finalizer returned from unsafeDumpToList was manually called after closeDB/shutDownDBS."
           mdb_txn_commit txn
    xs <- unfoldWhileM (\(_,b) -> b) $ alloca $ \pkey -> alloca $ \pval -> do
        bFound <- mdb_cursor_get MDB_NEXT cursor pkey pval 
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

