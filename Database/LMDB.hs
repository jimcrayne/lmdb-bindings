-- | This module provides two interfaces:
--
-- 1. A type-safe interface centered around the 'DBEnv' and 'DB' datatypes. It
-- should only be used with databases that it was used to create.  Rather than
-- the LMDB structure, the database is represented as a collection of mutable
-- references ('DBRef') and mutable lookup tables ('Single' and 'Multi').
--
-- 2. An untyped interface centered around the 'DBS' and 'DBHandle' datatypes.
-- It is useful for operating on arbitrary LMDB environments and debugging.
-- This is the interface used by the @lmdbtool@ utility.
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE CPP #-}
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE DeriveFunctor       #-}
{-# LANGUAGE KindSignatures      #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell     #-}
module Database.LMDB

    ( module Database.LMDB.Macros
    , LMDB_Error(..)

    -- * The Typed DB Monad Interface

    -- ** The Database Evironment
    , DBEnv -- (..)
    , openDBEnv
    , closeDBEnv
    , runDB
    , tryDB
    , unsafeGetDBS

    -- ** Specifying a Transaction
    , DB
    , orElse
    , cases
    , abort
    , dbtrace
    , dbtrac
    , dblift
    , transactionTime
    , withRNG

    -- ** Persistent mutable refs
    , readDBRef
    , writeDBRef

    -- ** Simple key/value tables
    --
    -- | Single-valued maps come in two flavors.  The 'BoundedKey' (or
    -- equivelently, 'BoundedKeyValue') flavor uses a vanilla LMDB table with
    -- keys and values straight-forwardly serialized and keys must adhere to
    -- LMDB limitations (typically a 511 byte maximum).  The 'HashedKey' flavor
    -- performs a 64-bit murmer hash on the key and uses the result as the LMDB
    -- key.  For each hashed key, a list of triples of the form (serialized
    -- key, 32-bit value length, serialized value) is stored which allows the
    -- implementation to account for hash collisions.
    , initSingle
    , testSingle
    , store
    , unstore
    , fetch
    , fetchByPrefix

    -- ** Multi-valued key/value tables
    --
    -- | Multi-valued maps come in three flavors.
    --
    -- The 'BoundedKey' flavor straight-forwardly serializes haskell valued
    -- keys to use LMDB keys.  All values for a given key are stored as a
    -- single LMDB value which consist of the concatenated serialization.  Note
    -- that the 'Binary' serialization instance is assumed to be able to parse
    -- a value from the front of a 'L.ByteString' without consuming the
    -- remaining data.  Keys must meet the LMDB requirements (typically a 511
    -- byte maximum).
    --
    -- The 'BoundedKeyValue' flavor uses LMDB's DUPSORT feature to implement
    -- the multi-map.  Both keys and values are straight forwardly serialized
    -- and both must meet LMDB's rquirements (typically a 511 byte maximum).
    --
    -- The 'HashedKey' flavor is implemented, on disk, exactly like a 'Single'
    -- 'HashedKey' table.  The difference is that 'insert' and 'remove' will
    -- allow multiple values per key.
    , initMulti
    , testMulti
    , insert
    , remove
    , fetchByPrefixMultiple
    , fetchMultiple

    -- ** Timestamps and randomness
    , TimeStamp
    , toSeconds
    , fromSeconds
    , Period(..)
    , addPeriod
    , currentTime
    , RNG(..)
    , makeGen
#if defined(VERSION_crypto_random)
    , getRandomBytes
#endif

    -- * The Untyped DBS Interface

    -- ** Public interface.
    -- *** Environments
    , DBS
    , withDBSDo
    , withManyDBSDo
    , withDBSCreateIfMissing
    , initDBS
    , openDBS
    , shutDownDBS 
    -- **** Already open?
    , isOpenEnv
    , listEnv
    , isOpenEnv'
    , listEnv'
    -- **** High Level Functions
    --
    -- | These functions are similar to the operations facilitated by
    --   the @lmdbtool@ utility. The unticked variants which open and
    --   close the environment are provided for when you only want to
    --   do one operation. (See '<#HLAF Higher Level Atomic Functions>'). (anchor #HLF#)
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
    -- *** Tables
    , createDB
    , createAppendDB
    , openDB
    , openAppendDB
    , createDupSortDB
    , createAppendDupDB
    , openDupSortDB
    , openAppendDupDB
    , unnamedDB
    , closeDB
    -- **** Editing tables
    , dropDB
    , delete
    , add
    -- **** Table properties
    , lengthDB
    -- *** Unsafe Functions
    --
    -- |   These functions may result in dangling pointers if not used with care.
    --
    , unsafeFetch
    , unsafeFetchW
    , unsafeDumpToList
    , unsafeDumpToListOp

    -- ** Internal Functions
    --
    -- |  These functions expose aspects of the interface that are subject to change.
    --    They are provided, but please prefer the public interface.
    --
    , internalGetDBFlags
    , internalOpenDB
    , openAppendDBFlags
    -- * High Level Atomic functions
    --
    -- | These functions use file paths and names as parameters.
    --   They do all the work of opening and closing the environment.
    --   They are similar to the commands of the  @lmdbtool@ utility
    --   (If you prefer to leave the environment open use functions under
    --   the heading <#HLF Higher Level Functions>.) #HLAF#
    --
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
    , copyTable
    -- * Global MVars
    -- | This is originally from the global-variables package, but it's not maintained.
    --   Open LMDB Enironments are in a hashtable stored in "LMDB Environments" MVar.
    , declareMVar
    , HashTable
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
-- import Data.Global
import System.IO.Unsafe
import qualified Data.HashTable.IO as H
import Data.String
import Data.Typeable
import qualified Data.HashTable.Class as Class
import qualified Data.HashTable.ST.Basic as Basic
-- import Control.Monad.Primitive
--import Control.Monad.Extra
import Control.DeepSeq (force)


-- Imports from Joe's interface
-- no attempt to remove redundancy
import           Control.Exception
import qualified Control.Concurrent.STM as STM
import Control.Concurrent.STM.TVar
import Control.Concurrent.MVar
import Control.Monad
import Database.LMDB.Raw
import Control.Applicative
import Control.DeepSeq
import           System.DiskSpace                  (getAvailSpace)
import Data.Data
import Data.Traversable (Traversable, traverse)
import Data.List (foldl1',partition)
-- import qualified Data.ByteString                   as S
import qualified Data.ByteString.Char8 as Char8
import qualified Data.ByteString.Internal          as S
import qualified Data.ByteString.Lazy as L
import Data.Int
import Data.Binary (encode,decode,Binary)
import Data.Binary.Get (runGet)
import Data.Bits
#ifdef NOHOURGLASS
import Data.Time -- UTCTime
import Data.Time.Clock.POSIX (posixSecondsToUTCTime,utcTimeToPOSIXSeconds)
#else
import Foreign.C.Types (CTime(..))
import Data.Hourglass (Period(..))
import qualified Data.Hourglass as Hourglass
import qualified System.Hourglass as Hourglass
#endif
import Data.Tuple
import Data.Word
import Data.Maybe
import Control.DeepSeq
import qualified Data.Generics.Aliases             as Generics
import qualified Data.Generics.Schemes             as Generics
import           Foreign.C.Types                   (CSize, CULong (..))
-- import           Foreign.ForeignPtr
import qualified Foreign.Concurrent
import           Foreign.Ptr
import System.IO.Unsafe (unsafePerformIO)
import Data.Global.Internal
import Language.Haskell.TH
import qualified Data.Digest.Murmur64 as Murmur
#if MIN_VERSION_base(4,8,0)
import Foreign.Marshal hiding (void)
#else
import Foreign.Marshal.Safe hiding (void)
#endif
import Foreign.Storable
import           System.IO.Unsafe                  (unsafeInterleaveIO)
import Database.LMDB.Macros
import Database.LMDB.Raw.Types
import Crypto.Random
import Control.Arrow (second, (***))
import Database.LMDB.BinaryUtil
import PackUtf8

-- | This type alias comes in two flavors depending on the build settings.  By
-- default, it will refer to Vincent Hanquez's 'Data.Hourglass.DateTime', but
-- if the "hourglass" build flag is disabled, it will instead be
-- 'Data.Time.UTCTime'.
--
-- Either way, you can use 'toSeconds' and 'fromSeconds' to convert to and from
-- an 'Int64' count of seconds since the epoch.
#ifdef NOHOURGLASS
type TimeStamp = UTCTime
data Period = Period { peroidYears :: !Int
                     , periodMonths :: !Int
                     , peroidDays :: !Int
                     }
toSeconds :: TimeStamp -> Int64
toSeconds utc = round $ utcTimeToPOSIXSeconds utc

fromSeconds :: Int64 -> TimeStamp
fromSeconds s = posixSecondsToUTCTime (realToFrac s)

currentTime :: IO TimeStamp
currentTime = getCurrentTime
#else
type TimeStamp = Hourglass.DateTime

toSeconds :: TimeStamp -> Int64
toSeconds vincentTime = t
  where (Hourglass.Elapsed (Hourglass.Seconds t)) = Hourglass.timeGetElapsed vincentTime

fromSeconds :: Int64 -> TimeStamp
fromSeconds t = Hourglass.timeFromElapsed (Hourglass.Elapsed (Hourglass.Seconds t))

currentTime :: IO TimeStamp
currentTime = Hourglass.dateCurrent
#endif


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
               } deriving Typeable

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

-- | like 'withDBSDo' but opens many environments at once
withManyDBSDo :: [FilePath] -> ([DBS] -> IO a) -> IO a
withManyDBSDo dirs action =
    bracket (mapM initDBS dirs)
            (mapM shutDownDBS)
            action

withDBSCreateIfMissing dir action = do
    createDirectoryIfMissing True dir
    withDBSDo dir action

foreign import ccall getPageSizeKV :: CULong

declareMVar "LMDB Environments"  h = _registryMVar
declareMVar s _ = error ("ERROR: Cannot fectch global MVar named " ++ show s)

{-# NOINLINE _registryMVar #-}
_registryMVar = unsafePerformIO $ (H.new >>= newMVar) :: MVar (HashTable ByteString DBS)
-- The above unsafePerformIO hack global reference could equivalently be declared using
-- a macro from Data.Global.Internal like so:
--
-- declare [t|MVar (HashTable ByteString DBS)|]
--         [| H.new >>= newMVar |]
--         "_registryMVar"

-- | Is this 'Filepath' a currently open LMDB Environment?
isOpenEnv :: FilePath -> IO Bool
isOpenEnv = isOpenEnv' . packUtf8

-- | Get list of open environments, using 'FilePath' type
listEnv = map unpackUtf8 <$> listEnv'

-- | Given a path encoded as a utf8 bytestring, does the current process have it as a currently open LMDB Environment?
isOpenEnv' :: S.ByteString -> IO Bool
isOpenEnv' dir = do
    h <- H.new :: IO (HashTable S.ByteString DBS)
    let registryMVar = declareMVar "LMDB Environments" h
    registry <- readMVar registryMVar
    result <- H.lookup registry dir
    case result of
        Nothing -> return False
        Just (DBS _ _ mvar) -> readMVar mvar

-- | Get list of open environment paths as Utf8 encoded ByteStrings
listEnv' :: IO [S.ByteString]
listEnv' = do
    h <- H.new :: IO (HashTable S.ByteString DBS)
    let registryMVar = declareMVar "LMDB Environments" h
    registry <- readMVar registryMVar 
    es <- H.toList registry 
    (stillOpen,closed) <- partitionM (readMVar . dbsIsOpen . snd) es
    mapM_ (H.delete registry . fst) closed
    return $ map fst stillOpen
    where
        -- | A version of 'partition' that works with a monadic predicate.
        --
        -- > partitionM (Just . even) [1,2,3] == Just ([2], [1,3])
        -- > partitionM (const Nothing) [1,2,3] == Nothing
        partitionM :: Monad m => (a -> m Bool) -> [a] -> m ([a], [a])
        partitionM f [] = return ([], [])
        partitionM f (x:xs) = do
            res <- f x
            (as,bs) <- partitionM f xs
            return ([x | res]++as, [x | not res]++bs)

-- | initDBS
--
--      dir - directory containing data.mdb, or an
--            existing empty directory if creating
--            a new environment
--
-- Start up and initialize whatever engine(s) provide access to the 
-- environment specified by the given path. 
initDBS :: FilePath -> IO DBS
initDBS dir = 
  initDBSOptions
            -- path to lmdb environment
            dir 

            -- maxreaders, I chose this arbitrarily.
            -- The vcache package doesn't set it.
            (Just 1000) 

            -- LDMB is designed to support a small handful of databases.
            -- i choose 10 (arbitarily) as maxdbs
            -- The vcache package uses 5
            (Just 10)

            [{-todo?(options)-}]

initDBSOptions :: FilePath -> Maybe Int -> Maybe Int -> [MDB_EnvFlag] -> IO DBS
initDBSOptions dir mbMaxReader mbMaxDb options = do
    h <- H.new :: IO (HashTable S.ByteString DBS)
    let registryMVar = declareMVar "LMDB Environments" h
    registry <- takeMVar registryMVar -- lock registry
    mbAlreadyOpen <- H.lookup registry (packUtf8 dir)
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
            H.insert registry (packUtf8 dir) retv
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

            case mbMaxReader of
                -- maxreaders, I chose this arbitrarily.
                Just x  -> mdb_env_set_maxreaders env x
                -- The vcache package doesn't set it, so I'll comment it out for now
                Nothing -> return ()

            -- LDMB is designed to support a small handful of databases.
            -- The vcache package uses 5
            case mbMaxDb of
                -- i choose x (arbitarily) as maxdbs
                Just x -> mdb_env_set_maxdbs env 10 
                -- i choose 10 (arbitarily) as maxdbs
                Nothing -> mdb_env_set_maxdbs env 10 

            mdb_env_open env dir options
            case maybeMVar of
                Nothing -> do
                    isopen <- newMVar True
                    return (env, isopen)
                Just mvar -> do
                    modifyMVar_ mvar (const $ return True) 
                    return (env, mvar)


-- | openDBS - shouldn't really need this
--
--  This is a no op in the case the environment is open, otherwise
--  it calls 'initDBS'.
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

-- | Like 'openDB', but specify LMDB flags.
--  This function is considered internal because it exposes the MDB_DbFlag type.
--  Consider using 'openDupSortDB' or 'createDupSortDB' ...
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

-- | unnamedDB dbs
-- get implicit main ‘database’.
unnamedDB ::  DBS -> IO DBHandle
unnamedDB (DBS env dir eMvar) = do 
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

-- | createDB db name
-- Opens the specified named database, creating one if it does not exist.
createDB ::  DBS -> ByteString -> IO DBHandle
createDB = internalOpenDB [MDB_CREATE]

-- | createAppendDB db name
-- Like 'createDB' but the data comparison function is set to always return 1
createAppendDB ::  DBS -> ByteString -> IO DBHandle
createAppendDB dbs name = openAppendDBFlags [MDB_CREATE] dbs name

-- | createDupSortDB db name
-- Like 'createDB' but the database is flagged DUPSORT
createDupSortDB ::  DBS -> ByteString -> IO DBHandle
createDupSortDB = internalOpenDB [MDB_CREATE, MDB_DUPSORT]

-- | createAppendDupDB db name
-- Like 'createDupSortDB' but the data comparison function is set to always return 1
createAppendDupDB ::  DBS -> ByteString -> IO DBHandle
createAppendDupDB dbs name = openAppendDBFlags [MDB_CREATE,MDB_DUPSORT] dbs name

bracketIfOpen mvar init close action = do
    b <- readMVar mvar
    if b then (bracket init close action)
         else (error "bracketIfOpen")

-- | openDB db name
-- Open a named database.
openDB ::  DBS -> ByteString -> IO DBHandle
openDB = internalOpenDB []

-- | openAppendDB db name
-- Open a named database with comparison function set to @\_ _ -> 1@.
openAppendDB ::  DBS -> ByteString -> IO DBHandle
openAppendDB dbs name = openAppendDBFlags [] dbs name

-- | openDupSortDB db name
-- Like 'openDB' but DUPSORT specified.
openDupSortDB ::  DBS -> ByteString -> IO DBHandle
openDupSortDB = internalOpenDB [MDB_DUPSORT]

-- | openAppendDupDB db name
-- Like 'openDupSortDB' but the data comparison function is set to always return 1
openAppendDupDB ::  DBS -> ByteString -> IO DBHandle
openAppendDupDB dbs name = openAppendDBFlags [MDB_DUPSORT] dbs name

-- | openAppendDBFlags
--  Like 'internalOpenDB' but sets the comparison function to always return 1.
--  This function is considered internal because it exposes the MDB_DbFlag type.
--  Consider using 'openAppendDupDB' or 'createAppendDupDB' ...
openAppendDBFlags :: [MDB_DbFlag] -> DBS -> ByteString -> IO DBHandle
openAppendDBFlags flags dbs name = do
    handle@(DBH (env,mvar0) dbi flags mvar1) <- internalOpenDB flags dbs name
    compare <- wrapCmpFn (\_ _ -> return 1)
    bracketIfOpen mvar0 (mdb_txn_begin env Nothing False)
            mdb_txn_commit
            (\txn -> mdb_set_dupsort txn dbi compare)
    return handle

-- openDBForCopy db name
-- Like 'openAppendDupDB' but with explicit flags, and comparsion function
openDBForCopy ::  DBS -> ByteString -> [MDB_DbFlag] -> FunPtr MDB_cmp_func -> IO DBHandle
openDBForCopy dbs name flags compare = do
    handle@(DBH (env,mvar0) dbi flags mvar1) <- internalOpenDB flags dbs name
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

-- | get the flags associated with a table
--  This function is considered internal because it exposes the MDB_DbFlag type.
internalGetDBFlags :: DBHandle -> IO [MDB_DbFlag]
internalGetDBFlags (DBH (env,eMvar) dbi writeflags mvar) = do
    eOpen <- readMVar eMvar
    if eOpen
      then do
        dbisopen <- readMVar mvar
        if dbisopen
            then bracket (mdb_txn_begin env Nothing False)
                         (mdb_txn_commit)
                         (\txn -> mdb_dbi_flags txn dbi)
            else return []
      else return []


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
unsafeFetch = unsafeFetchInternal True

-- | like 'unsafeFetch' but you might poke into the byte strings? anyway
--   this opens a write transaction, so locks writers until the strings
--   are finalized.
unsafeFetchW = unsafeFetchInternal False

unsafeFetchInternal :: Bool -> DBHandle -> ByteString -> IO (Maybe (ByteString, IO()) )
unsafeFetchInternal isReadOnly = \(DBH (env,eMvar) dbi _ mvar) key = do
    txn <- mdb_txn_begin env Nothing isReadOnly
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

-- | add db key value
-- Store a key-value pair in the database. Returns False
-- if the key already existed.
add :: DBHandle -> ByteString -> ByteString -> IO Bool
add (DBH (env,mv0) dbi writeflags _) key val =  -- todo check mvars
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
    db <- unnamedDB dbs
    (keysVals,final) <- unsafeDumpToListOp MDB_NEXT_NODUP db
    let keys = map (S.copy . fst) keysVals
    force keys `seq` final 
    return keys

listTables' dbs = bracket (unnamedDB dbs) closeDB $ \db -> do
    (keysVals,final) <- unsafeDumpToListOp MDB_NEXT_NODUP db
    let keys = map (S.copy . fst) keysVals
    force keys `seq` final 
    return keys

listTablesCreateIfMissing x = withDBSCreateIfMissing x $ \dbs -> do
    db <- unnamedDB dbs
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
    add d k v

insertKey' dbs n k v = bracket (createDB dbs n) closeDB $ \d -> add d k v

deleteKey x n k = withDBSDo x $ \dbs -> do
    d <- openDB dbs n
    delete d k 

deleteKey' dbs n k = bracket (openDB dbs n) closeDB $ \d -> delete d k

lookupVal x n k = withDBSDo x $ \dbs -> do
    d <- openDB dbs n
    mb <- unsafeFetch d k
    case mb of
        Just (val,final) -> do
            let x = S.copy val
            force x `seq` final
            return (Just x)
        Nothing -> return Nothing

lookupVal' dbs n k = bracket (openDB dbs n) closeDB $ \d -> do
    mb <- unsafeFetch d k
    case mb of
        Just (val,final) -> do
            let x = S.copy val
            force x `seq` final
            return (Just x)
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

-- | Copy a table (LMDB database) from one environment to another or
--   copy a table to a different table within an environment.
--
--   If this is to create a new environment, you may need to create
--   an empty directory first.
--
--  Parameters:
--      bAllowDuplicates - True to set comparision function to 1,
--                         thereby preserving order and allowing 
--                         duplicate key pairs. Usually
--                         you don't want this. But you probably
--                         do if you used 'openAppendDupDB'.
--      
--      dir1 tbl1   - Source environment(path) and table name
--      dir2 tbl2   - Destination environment(path) and table name
--
--  Returns:
--      [(bReplaced,(key,val)] -> list of all keys and values inserted
--                                paired with a boolean flag indicating
--                                if the key was already in the destination.
copyTable :: Bool -> FilePath -> S.ByteString -> FilePath -> S.ByteString -> IO [(Bool, (S.ByteString, S.ByteString))]
copyTable bAllowDuplicates dir1 tbl dir2 tbl2 | dir1 /= dir2 = withManyDBSDo [dir1,dir2] $ \[dbs1,dbs2] -> do
    d <- openDB dbs1 tbl
    flag <- internalGetDBFlags d
    let (<>) = S.append
    -- S.putStrLn (tbl <> S.pack " FLAGS(1): " <> S.pack (show flag))
    compare <- wrapCmpFn (\_ _ -> return 1)
    d2 <- if bAllowDuplicates then openDBForCopy dbs2 tbl2 (MDB_CREATE:flag) compare
                              else internalOpenDB (MDB_CREATE:flag) dbs2 tbl2
    (xs,final) <- unsafeDumpToList d
    let ys   = map copy xs
        copy (x,y) = (S.copy x, S.copy y)
    bools <- mapM (\(k,v) -> add d2 k v) ys
    return (zip bools ys)
copyTable bAllowDuplicates dir1 tbl dir2 tbl2 | dir1 == dir2 && (tbl /= tbl2 || bAllowDuplicates) = withDBSDo dir1 $ \dbs -> do
    d <- openDB dbs tbl
    flag <- internalGetDBFlags d
    let (<>) = S.append
    -- S.putStrLn (tbl <> S.pack " FLAGS(2): " <> S.pack (show flag))
    compare <- wrapCmpFn (\_ _ -> return 1)
    d2 <- if bAllowDuplicates then openDBForCopy dbs tbl2 (MDB_CREATE:flag) compare
                              else internalOpenDB (MDB_CREATE:flag) dbs tbl2
    (xs,final) <- unsafeDumpToList d
    let ys   = map copy xs
        copy (x,y) = (S.copy x, S.copy y)
    bools <- mapM (\(k,v) -> add d2 k v) ys
    return (zip bools ys)
copyTable False dir1 tbl dir2 tbl2 | dir1 == dir2 && tbl == tbl2 = do
    xs <- toList dir1 tbl
    return $ zip (repeat False) xs

------------------------------------------
--  Static Typed Database Interface

-- | Monad representing a database transaction
--
-- If the user avoids the 'Monad' bind operation and restricts himself to the
-- 'Applicative' and 'cases' interface, then the following will be
-- automatically detected:
--
--  * whether or not the transaction requires write-access
--
--  * whether or not the transaction requires entropy
--
--  * whether or not the transaction must be supplied a time-stamp
--
-- If '>>=' is used, then 'runDB' will meet all three of those conditions
-- regardless if they are actually necessary.
data DB a = DB
    { dbFlags :: !Word8 -- three bit flags, 1 - uses write permission, 2 - uses timestamp, 4 - uses entropy
    , dbOperation :: DBParams -> MDB_txn -> IO (Either LMDB_Error a)
    }
 deriving Functor

data DBParams = DBParams
    { dbtime :: TimeStamp
    , dbRNG :: MVar RNG
    }

dbRequiresWrite :: DB a -> Bool
dbRequiresWrite a = dbFlags a .&. 1 /= 0

dbRequiresStamp :: DB a -> Bool
dbRequiresStamp a = dbFlags a .&. 2 /= 0

instance Applicative DB where
    pure x = DB 0 $ \_ _ -> return $ Right x
    f <*> a = DB (dbFlags f .|. dbFlags a) $ \stamp txn -> do
        ef <- dbOperation f stamp txn
        case ef of
            Left er -> return $ Left er
            Right ff -> do
                ea <- dbOperation a stamp txn
                case ea of
                    Left er -> return $ Left er
                    Right aa -> return $ Right $ ff aa

instance Monad DB where
    return x = pure x
    a >> b = fmap (flip const) a <*> b

    -- The monad >>= operation forces use of a read/write transaction.
    -- Use the Applicative interface for read-only.
    a >>= f = DB 7 $ \stamp txn -> do
        ea <- dbOperation a stamp txn
        case ea of
            Left er -> return $ Left er
            Right aa -> dbOperation (f aa) stamp txn

-- | Obtain a time-stamp for the current transaction.  Note that repeated calls
-- to this within a single transaction will always return the same value.
--
-- Note that this timestamp is, unfortunately, the time a transaction is
-- *started*.  Transactions needn't actually be completed in the same order
-- that they are started.  A transaction cannot obtain it's own completion
-- time.
transactionTime :: DB TimeStamp
transactionTime = DB 2 $ \DBParams {dbtime=stamp} _ -> return $ Right stamp

-- | This function adjusts a 'TimeStamp' using a human-friendly time delta.  It
-- is useful for checking expiration dates or freshness requirements for
-- cryptographic signatures.
addPeriod :: TimeStamp -> Period -> TimeStamp
addPeriod now period =
#ifdef NOHOURGLASS
    let now' = utcToLocalTime utc now
        day0 = localDay now'
        Period y m d = period
        day1 = addDays (fromIntegral d)
                $ addGregorianMonthsClip (fromIntegral m)
                $ addGregorianYearsClip (fromIntegral y) day0
        end_stamp = localTimeToUTC utc (now' { localDay = day1 })
#else
    let end_date = Hourglass.dateAddPeriod (Hourglass.dtDate now) period -- Period years months days
        end_stamp = Hourglass.DateTime end_date $ Hourglass.dtTime now
#endif
    in end_stamp


dbError :: String -> String -> LMDB_Error
dbError ctx message = LMDB_Error ctx message (Right MDB_INVALID)


-- | Like STM\'s 'Control.Monad.STM.retry', but with a failure message.
abort :: String -> DB a
abort message = DB 0 $ \_ _ -> return $ Left er
 where
    er = dbError "aborted" message

-- | This is similar to STM\'s 'Control.Monad.STM.orElse'. It is often used to
-- handle lookup failures for 'Single' and 'Multi' tables.
orElse :: DB a -> DB a -> DB a
orElse a b = DB (dbFlags a .|. dbFlags b) $ \stamp txn -> do
    -- Note: orElse needs to use a subtransaction unless the expression
    --  is read-only.
    txn' <- if dbRequiresWrite a
                then mdbTry $ mdb_txn_begin (mdb_txn_env txn) (Just txn) False
                else return $ Right txn
    either (return . Left) (withTxn stamp txn) txn'
 where
    withTxn stamp txn txn' = do
        ea <- handle (return . Left) $ dbOperation a stamp txn'
        if dbRequiresWrite a
            then case ea of
                    Left _ -> do mdb_txn_abort txn'
                                 dbOperation b stamp txn
                    Right x -> do mdb_txn_commit txn'
                                  return $ Right x
            else either (const $ dbOperation b stamp txn) (return . Right) ea

-- | A common situation is to perform an if/then branch based on the results of
-- prior 'DB' operations.  Since 'Bool' implements 'Bounded' and 'Enum', we can
-- use 'cases' to evaluate both possibilities ahead of time (before any
-- database operations are performed) and determine that in all cases, the
-- transaction remains read-only.  Thus the write flag needn't be specified for
-- the underlying LMDB transaction.
--
-- This function is called "cases" because it provides control functionality
-- similar to a case ... of ... expression.  The second argument might be
-- elegantly specified using the \"LambdaCase\" haskell syntax.
--
-- The astute reader will notice that its type is similar to that of '>>=' but
-- with restrictions that the input implement 'Bounded' and 'Enum'.  This
-- similarity is intentional.  Use this instead of '>>=' if you want to avoid
-- setting the write-acces, entropy, and timestamp flags unneccessarily.
cases :: forall a x. (Bounded a, Enum a) => DB a -> (a -> DB x) -> DB x
cases toggle op = DB flags $ \stamp txn -> do
    ei <- dbOperation toggle stamp txn
    either (return . Left) (\a -> dbOperation (op a) stamp txn) ei
 where
    flags = foldl1' (.|.) $ map (dbFlags . op) posibilities
    posibilities = [minBound .. maxBound] :: [a]


buildByteString :: MDB_val -> IO S.ByteString
buildByteString (MDB_val size ptr) = do
    fptr <- newForeignPtr_ ptr
    return $ S.fromForeignPtr fptr 0 (fromIntegral size)

-- arguments:
--
-- [ bs ]     Input bytestring to argument: action.
--
-- [ decode ] Post-process each result of action.
--
-- [ action ] Operation on MDB_val to container of MDB_vals.
--
-- Note: Any lazy-IO is destroyed here by invoking 'traverse'.
withMDB :: forall f a. Traversable f => S.ByteString -> (S.ByteString -> a) -> (MDB_val -> IO (f MDB_val)) -> IO (f a)
withMDB bs decodeS action = do
    let (fptr,offset,len) = S.toForeignPtr bs
    withForeignPtr fptr $ \ptr -> do
        mb <- action (MDB_val (fromIntegral len) (ptr `plusPtr` offset))
        traverse decodeMDB mb
 where
    decodeMDB :: MDB_val -> IO a
    decodeMDB (MDB_val len ptr) = do
        fptr <- newForeignPtr_ ptr
        return $ decodeS $ S.fromForeignPtr fptr 0 (fromIntegral len)

withMDB_ :: S.ByteString -> (MDB_val -> IO a) -> IO a
withMDB_ bs action = do
    let (fptr,offset,len) = S.toForeignPtr bs
    withForeignPtr fptr $ \ptr -> do
        action (MDB_val (fromIntegral len) (ptr `plusPtr` offset))


-- | Read a 'DBRef' value from a database.
--
-- If the database is new or this value has never been written, then the
-- transaction will fail.
--
-- A global 'DBRef' can be declared using the template-haskell 'database'
-- macro.  For example, to decare a boolean reference named "myref", use the
-- following:
--
-- > database [d| myref = xxx :: DBRef Bool |]
--
readDBRef :: Binary a => DBRef a -> DB a
readDBRef (DBRef keyname dbivar) = DB 0 $ \_ txn -> do
    dbi <- performAtomicIO dbivar $ mdb_dbi_open txn Nothing []
    maybe (Left $ dbError "readDBRef" "bad reference")
          Right
       <$> withMDB keyname decodeStrict (mdb_get txn dbi)

-- | Write a 'DBRef'' value to a database.
--
-- If the database is new or this value has never been written, then the
-- 'DBRef' entry will be created in the database.  See 'readDBRef' for how to
-- declare a 'DBRef' variable for use with this function.
writeDBRef :: Binary a => DBRef a -> a -> DB ()
writeDBRef (DBRef keyname dbivar) val = DB 1 $ \_ txn -> do
    dbi <- performAtomicIO dbivar $ mdb_dbi_open txn Nothing []
    let flags  = compileWriteFlags [] -- TODO: ?
    _ <- withMDB_ keyname $ \key -> do
        let (fptr,offset,len) = S.toForeignPtr $ encodeStrict val
        withForeignPtr fptr $ \ptr -> do
            let valmdb = MDB_val (fromIntegral len) (ptr `plusPtr` offset)
            mdb_put flags txn dbi key valmdb
    return $ Right ()

-- | Lookup a value from a given key using a table created by 'initSingle'.
-- The transaction will fail if the key does not exist in the table.
fetch :: forall f k v. (Eq k, Binary k, Binary v, FlavorKind f) => Single f k v -> k -> DB v
fetch (Single dbname dbivar) k =
    case flavor (Proxy :: Proxy f) of
        BoundedKey      -> DB 0 $ \_ txn -> _bounded txn
        BoundedKeyValue -> DB 0 $ \_ txn -> _bounded txn
        HashedKey       -> DB 0 $ \_ txn -> _hashed txn
 where
    _bounded txn = do
            dbi <- performAtomicIO dbivar $ mdb_dbi_open txn (Just $ dbname) []
            maybe (Left $ dbError "fetch" "key not found")
                  Right
               <$> withMDB (encodeStrict k) decodeStrict (mdb_get txn dbi)
    _hashed txn = do
            dbi <- performAtomicIO dbivar $ mdb_dbi_open txn (Just $ dbname) []
            let encodedKey = encode k
                w = Murmur.asWord64 $ Murmur.hash64 encodedKey
            mb <- with w $ \pw -> do
                let mdbkey = MDB_val 8 (castPtr pw)
                mbs <- mdb_get txn dbi mdbkey >>= traverse buildByteString
                return $ mbs >>= runGet (findForKey k) . L.fromChunks . (:[])
            maybe (Left $ dbError "fetch" "key not found")
                  Right
                   <$> return mb

findMatchHashed :: forall v. Binary v
                => L.ByteString -> MDB_cursor -> Ptr MDB_val -> Ptr MDB_val -> MDB_cursor_op -> IO (Maybe v)
findMatchHashed encodedKey cursor pkey pval cursor_opt = do
    bFound <- mdb_cursor_get cursor_opt cursor pkey pval
    if not bFound
        then return Nothing
        else do
            -- MDB_val klen kp <- peek pkey
            MDB_val vlen vp <- peek pval
            -- fkp <- newForeignPtr_ kp
            fvp <- newForeignPtr_ vp
            -- Values are stored as (S.ByteString,valueType)
            --  so that we can handle hash collisions.
            let -- hkey = S.fromForeignPtr fkp 0 (fromIntegral klen)
                (key,val) = decodeStrict $ S.fromForeignPtr fvp 0 (fromIntegral vlen)
            -- TODO: Should we use Eq instead of L.ByteString comparision?
            if L.fromChunks [key] == encodedKey -- encode (key :: k) == encodedKey
                then return $ Just val
                else findMatchHashed encodedKey cursor pkey pval MDB_NEXT_DUP

-- | Remove a key/value pair from a table that was created with 'initSingle'.
unstore :: forall f k v. (Eq k, Binary k, Binary v, FlavorKind f) => Single f k v  -> k -> DB ()
unstore (Single dbname dbivar) k =
    case flavor (Proxy :: Proxy f) of
        BoundedKey      -> DB 1 $ _del_bounded dbname dbivar k Nothing []
        BoundedKeyValue -> DB 1 $ _del_bounded dbname dbivar k Nothing []
        HashedKey       -> DB 1 $ _del_hashed dbname dbivar k Nothing

_del_bounded ::
    (Binary k, Eq k) =>
    MapName -> TVar (Pending MDB_dbi) -> k -> Maybe MDB_val -> [MDB_DbFlag]
        -> DBParams -> MDB_txn -> IO (Either a1 ())
_del_bounded dbname dbivar k mbval flags _ txn = do
            dbi <- performAtomicIO dbivar $ mdb_dbi_open txn (Just $ dbname) flags
            _ <- withMDB_ (encodeStrict k) $ \key -> do
                    mdb_del txn dbi key mbval
            return $ Right ()

_del_hashed ::
    (Binary k, Eq k) =>
    MapName -> TVar (Pending MDB_dbi) -> k -> Maybe S.ByteString
        -> DBParams -> MDB_txn -> IO (Either a ())
_del_hashed dbname dbivar k mbval _ txn = do
            dbi <- performAtomicIO dbivar $ mdb_dbi_open txn (Just $ dbname) []
            let encodedKey = encode k
                w = Murmur.asWord64 $ Murmur.hash64 encodedKey
            with w $ \pw -> do
                let key = MDB_val 8 (castPtr pw)
                mbs <- mdb_get txn dbi key >>= traverse buildByteString
                case runGet (splitKeyChunks k) $ L.fromChunks $ maybeToList mbs of
                    []  -> {- case mbval of -}
                            {- Nothing  -> -} void $ mdb_del txn dbi key Nothing
                            {- Just val -> void $ withMDB_ val (mdb_del txn dbi key . Just) -}
                    kvs -> do
                        let isKeeper = case mbval of
                                        Nothing -> \(k', _)  -> k'/=k
                                        Just v  -> \(k', v') -> k'/=k || v'/=L.fromChunks [v]
                            (keepers,losers) = partition isKeeper kvs
                            bs = L.concat $ map snd $ keepers
                        when (not $ null losers) $ do
                            if L.null bs
                              then void $ mdb_del txn dbi key Nothing
                              else withMDB_ (S.concat $ L.toChunks bs) $ \val -> do
                                void $ mdb_put (compileWriteFlags []) txn dbi key val
            return (Right ())


-- | Store a key/value pair into a table.  If the key already existed, it will
-- be overwritten.  See 'initSingle' for how to declare a table of type
-- 'Single'.
store :: forall f k v. (Eq k, Binary k, Binary v, FlavorKind f) => Single f k v -> k -> v -> DB ()
store (Single dbname dbivar) k val =
    case flavor (Proxy :: Proxy f) of
        BoundedKey      -> DB 1 $ \_ txn -> _bounded txn
        BoundedKeyValue -> DB 1 $ \_ txn -> _bounded txn
        HashedKey       -> DB 1 $ \_ txn -> _hashed txn
 where
    _bounded txn = do
            dbi <- performAtomicIO dbivar $ mdb_dbi_open txn (Just $ dbname) []
            let flags  = compileWriteFlags [] -- TODO: ?
            _ <- withMDB_ (encodeStrict k) $ \key -> do
                let (fptr,offset,len) = S.toForeignPtr $ encodeStrict val
                withForeignPtr fptr $ \ptr -> do
                    let valmdb = MDB_val (fromIntegral len) (ptr `plusPtr` offset)
                    mdb_put flags txn dbi key valmdb
            return $ Right ()

    _hashed txn = do
            dbi <- performAtomicIO dbivar $ mdb_dbi_open txn (Just $ dbname) []
            let flags  = compileWriteFlags [] -- TODO: ?
                encodedKey = encode k
                w = Murmur.asWord64 $ Murmur.hash64 encodedKey
            with w $ \pw -> do
                let key = MDB_val 8 (castPtr pw)
                mbs <- mdb_get txn dbi key >>= traverse buildByteString
                let bs = L.fromChunks (maybeToList mbs)
                    kvs = runGet (splitKeyChunks k) bs
                    dups = filter (\(storedk,_) -> storedk==k) kvs
                when (null dups) $ do
                    let (fptr,offset,len) = S.toForeignPtr $ S.concat $ L.toChunks $ appendKeyValue bs k val
                    withForeignPtr fptr $ \ptr -> do
                        let valmdb = MDB_val (fromIntegral len) (ptr `plusPtr` offset)
                        void $ mdb_put flags txn dbi key valmdb
                return $ Right ()

-- | This is the multi-map equivlent of 'fetch'.  It returns the list of values
-- associated with a given key.  Unlike 'fetch', this function does not fail
-- the transaction when the key has no associations in the table.  Instead, it
-- returns an empty list.
fetchMultiple :: forall f k v. (Eq k, Binary k, Binary v, FlavorKind f) => Multi f k v -> k -> DB [v]
fetchMultiple (Multi dbname dbivar) k =
    case flavor (Proxy :: Proxy f) of
        BoundedKeyValue -> DB 0 $ \_ txn -> dupsortFetch dbname dbivar txn k
        BoundedKey ->  DB 0 $ \_ txn -> do
            dbi <- performAtomicIO dbivar $ mdb_dbi_open txn (Just $ dbname) []
            maybe (Right [])
                  Right
               <$> withMDB (encodeStrict k) (runGet getMany . L.fromChunks . (:[]) ) (mdb_get txn dbi)
        HashedKey -> DB 0 $ \_ txn -> do
            dbi <- performAtomicIO dbivar $ mdb_dbi_open txn (Just $ dbname) []
            let encodedKey = encode k
                w = Murmur.asWord64 $ Murmur.hash64 encodedKey
            vs <- with w $ \pw -> do
                let mdbkey = MDB_val 8 (castPtr pw)
                mbs <- mdb_get txn dbi mdbkey >>= traverse buildByteString
                return $ maybeToList mbs >>= runGet (findManyForKey k) . L.fromChunks . (:[])
            return $ Right vs

dupsortFetch :: forall k v. (Eq k, Binary k, Binary v) => MapName -> TVar (Pending MDB_dbi) -> MDB_txn -> k -> IO (Either LMDB_Error [v])
dupsortFetch dbname dbivar txn k = do
    dbi <- performAtomicIO dbivar $ mdb_dbi_open txn (Just $ dbname) [MDB_DUPSORT]
    let encodedKey = encode k
        (fptr,offset,len) = S.toForeignPtr $ S.concat $ L.toChunks encodedKey
    withForeignPtr fptr $ \ptr -> do
        cursor <- mdb_cursor_open txn dbi
        alloca $ \pkey -> alloca $ \pval -> do
            poke pkey $ MDB_val (fromIntegral len) (ptr `plusPtr` offset)
            Right <$> findMatch encodedKey cursor pkey pval MDB_FIRST_DUP
 where
    decodeValue fvp vlen =
        -- TODO: should be using 'unsafePackCStringLen' here.
        Just $ decodeStrict $ S.fromForeignPtr fvp 0 (fromIntegral vlen)

    findMatch :: forall v. Binary v => L.ByteString -> MDB_cursor -> Ptr MDB_val -> Ptr MDB_val -> MDB_cursor_op -> IO [v]
    findMatch encodedKey cursor pkey pval cursor_opt = do
        bFound <- mdb_cursor_get cursor_opt cursor pkey pval
        if not bFound
            then do
                -- TODO: ensure the cursor is closed; maybe do without lazy IO.
                mdb_cursor_close cursor
                return []
             else do
                MDB_val vlen vp <- peek pval
                fvp <- newForeignPtr_ vp
                maybe (findMatch encodedKey cursor pkey pval MDB_NEXT_DUP)
                      (\val -> do
                        vals <- unsafeInterleaveIO $ findMatch encodedKey cursor pkey pval MDB_NEXT_DUP
                        return (val : vals))
                    $ decodeValue fvp vlen


-- | Associate a value with a given key in a multi-map table.  The table should
-- first have been created with 'initMulti'.  This is the multi-map equivelent
-- of the single-map 'store'.  There is currently no multi-map equevelent for
-- 'unstore'.  TODO: implement a method for removing items from a multi-map.
insert :: forall f k v. (Eq k, Binary k, Binary v, FlavorKind f) => Multi f k v -> k -> v -> DB ()
insert (Multi dbname dbivar) k val =
    case flavor (Proxy :: Proxy f) of
        BoundedKey ->  DB 1 $ \_ txn -> do
            dbi <- performAtomicIO dbivar $ mdb_dbi_open txn (Just $ dbname) []
            withMDB_ (encodeStrict k) $ \key -> do
                mbs <- mdb_get txn dbi key >>= traverse (fmap (L.fromChunks . (:[])) . buildByteString)
                let xbs :: [(v,L.ByteString)]
                    xbs = maybe [] (runGet getManyChunks) mbs
                    encoded = encode val
                    dups = filter (\(_,storedv) -> storedv==encoded) xbs
                when (null dups) $ do
                    let newval = maybe encoded (`L.append` encoded) $ mbs
                        (fptr,offset,len) = S.toForeignPtr $ S.concat $ L.toChunks newval
                    withForeignPtr fptr $ \ptr -> do
                        let valmdb = MDB_val (fromIntegral len) (ptr `plusPtr` offset)
                        void $ mdb_put (compileWriteFlags []) txn dbi key valmdb
            return $ Right ()

        BoundedKeyValue -> DB 1 $ \_ txn -> do
            dbi <- performAtomicIO dbivar $ mdb_dbi_open txn (Just $ dbname) [MDB_DUPSORT]
            let flags  = compileWriteFlags [] -- TODO: ?
            _ <- withMDB_ (encodeStrict k) $ \key -> do
                let (fptr,offset,len) = S.toForeignPtr $ encodeStrict val
                withForeignPtr fptr $ \ptr -> do
                    let valmdb = MDB_val (fromIntegral len) (ptr `plusPtr` offset)
                    mdb_put flags txn dbi key valmdb
            return $ Right ()

        HashedKey -> DB 1 $ \_ txn -> do
            dbi <- performAtomicIO dbivar $ mdb_dbi_open txn (Just $ dbname) []
            let flags  = compileWriteFlags [] -- TODO: ?
                encodedKey = encode k
                w = Murmur.asWord64 $ Murmur.hash64 encodedKey
            with w $ \pw -> do
                let key = MDB_val 8 (castPtr pw)
                mbs <- mdb_get txn dbi key >>= traverse buildByteString
                let bs = L.fromChunks (maybeToList mbs)
                    kvs = runGet (splitKeyChunks k) bs
                    encoded = encode val
                    dups = filter (\(storedk,storedv) -> storedk==k && storedv==encoded ) kvs
                when (null dups) $ do
                    let (fptr,offset,len) = S.toForeignPtr $ S.concat $ L.toChunks $ appendKeyValue bs k val
                    withForeignPtr fptr $ \ptr -> do
                        let valmdb = MDB_val (fromIntegral len) (ptr `plusPtr` offset)
                        void $ mdb_put flags txn dbi key valmdb
                return $ Right ()

-- | Remove a key/value pair from a table that was created with 'initMulti'.
--
-- If 'Nothing' is specified for the value, then all values associated with the
-- given key will be removed.  Otherwise, only matching values will be removed.
remove :: forall f k v. (Eq k, Binary k, Binary v, FlavorKind f) => Multi f k v -> k -> Maybe v -> DB ()
remove (Multi dbname dbivar) k mbval =
    case flavor (Proxy :: Proxy f) of
        BoundedKey      -> case mbval of
            Nothing  -> DB 1 $ _del_bounded dbname dbivar k Nothing []
            Just val -> DB 1 $ \params txn -> do
                dbi <- performAtomicIO dbivar $ mdb_dbi_open txn (Just $ dbname) []
                withMDB_ (encodeStrict k) $ \key -> do
                    mbs <- mdb_get txn dbi key >>= traverse (fmap (L.fromChunks . (:[])) . buildByteString)
                    let xbs :: [(v,L.ByteString)]
                        xbs = maybe [] (runGet getManyChunks) mbs
                        encoded = encode val
                        keepers = filter (\(_,storedv) -> storedv/=encoded) xbs
                    case keepers of
                        [] -> _del_bounded dbname dbivar k Nothing [] params txn
                        ks -> withMDB_ (L.toStrict $ L.concat $ map snd ks) $ \valmdb -> do
                                void $ mdb_put (compileWriteFlags []) txn dbi key valmdb
                                return $ Right ()

        BoundedKeyValue -> case mbval of
            Nothing  -> DB 1 $ _del_bounded dbname dbivar k Nothing [MDB_DUPSORT]
            Just val -> DB 1 $ \params txn -> do
                withMDB_ (encodeStrict val) $ \valmdb -> do
                    _del_bounded dbname dbivar k (Just valmdb) [MDB_DUPSORT] params txn

        HashedKey       -> case mbval of
            Nothing  -> DB 1 $ _del_hashed dbname dbivar k Nothing
            Just val -> DB 1 $ _del_hashed dbname dbivar k (Just $ encodeStrict val)

-- | Create an empty multi-map table.  Use the 'database' template-haskell
-- macro to declare a table like so:
--
-- > databaes [d| public_bindings = xxx :: Multi 'BoundedKey Nickname RawCertificate |]
--
-- The above example would declare a variable "public_bindings" that refers to
-- a table associates a list of "RawCertificate" values with a given "Nickname"
-- The 'BoundedKey' 'DBFlavor' indicates that keys will serialized and used
-- directly and must meet the (usually 511-byte) conditions that LMDB imposes.
initMulti :: forall f k v. FlavorKind f => Multi f k v -> DB ()
initMulti (Multi dbname dbivar) = DB 1 $ \_ txn -> do
    let flags = case flavor (Proxy :: Proxy f) of
                    BoundedKeyValue -> [MDB_CREATE,MDB_DUPSORT]
                    _               -> [MDB_CREATE]
    _ <- performAtomicIO dbivar $ mdb_dbi_open txn (Just $ dbname) flags
    return $ Right ()

-- | Create an empty lookup table within a database.  The table needs to be
-- declared using the 'database' template-haskell macro.  For example,
--
-- > database [d| freshness = xxx :: Single 'HashedKey RawCertificate Int64 |]
--
-- The above example would declare a variable "freshness" that refers to a
-- table that obtains 64-bit integer values given a value of type
-- 'RawCertificate'.  The 'HashedKey' 'DBFlavor' indicates that keys will
-- actually be hashed for efficency of lookups and to work-around LMDB's
-- hard-coded maximum length.
initSingle :: forall f k v. FlavorKind f => Single f k v -> DB ()
initSingle (Single dbname dbivar) = DB 1 $ \params txn -> do
    let flags = case flavor (Proxy :: Proxy f) of
                    BoundedKey      -> [MDB_CREATE]
                    BoundedKeyValue -> [MDB_CREATE]
                    HashedKey       -> [MDB_CREATE]
    _ <- performAtomicIO dbivar $ mdb_dbi_open txn (Just $ dbname) flags
    return $ Right ()

-- | Fail the transaction if the given table does not exist.  It is assumed the
-- table, if it exists, was created by 'initMulti'.
testMulti :: forall f k v. FlavorKind f => Multi f k v -> DB ()
testMulti (Multi dbname dbivar) = DB 1 $ \_ txn -> do
    -- TODO: Should we handle exception and return Left ?
    let flags = case flavor (Proxy :: Proxy f) of
                    BoundedKeyValue -> [MDB_DUPSORT]
                    _               -> []
    _ <- performAtomicIO dbivar $ mdb_dbi_open txn (Just $ dbname) flags
    return $ Right ()

-- | Fail the transaction if the specified table does not exist.  See
-- 'initSingle' for how to declare a table using the 'database' macro.
--
-- This method is handy for validating a database is ready to use with your
-- application without having to go to the trouble of attempting a 'fetch'.
testSingle :: forall f k v. FlavorKind f => Single f k v -> DB ()
testSingle (Single dbname dbivar) = DB 1 $ \_ txn -> do
    -- TODO: Should we handle exception and return Left ?
    let flags = case flavor (Proxy :: Proxy f) of
                    BoundedKey      -> []
                    BoundedKeyValue -> []
                    HashedKey       -> []
    _ <- performAtomicIO dbivar $ mdb_dbi_open txn (Just $ dbname) flags
    return $ Right ()



-- don't export
mdbTry :: IO a -> IO (Either LMDB_Error a)
mdbTry action = handle (return . Left)
                       (fmap Right action)


-- | An opague type representing an open database.
data DBEnv = DBEnv DBS (MVar RNG)

-- | Use this to ignore database layout and insert and lookup
--   arbitrary byte strings with 'insertKey\'' and 'lookupVal\''
unsafeGetDBS :: DBEnv -> IO DBS
unsafeGetDBS (DBEnv dbs _) = return dbs

-- | Open a database.  The current design is limited in that data represented
-- by 'DBRef', 'Single', and 'Multi' are assumed to be associated with only a
-- single active 'DBEnv' so care must be taken if multiple databases are
-- opened.
--
-- The 'Maybe' argument indicates a path to a "noise" file which is used to
-- seed a pseudo-RNG en lieu of using true system entropy for the
-- 'getRandomBytes' interface of the 'DB' transactions.  This is useful for
-- making tests reproducable.
--
-- Note that it is neccessary to invoke 'closeDBEnv' to flush writes and
-- clean-up when you are finished with the database.
openDBEnv :: FilePath -> Maybe FilePath -> IO DBEnv
openDBEnv path mbnoise = do
        dbsEnv <-
          initDBSOptions
            -- path to lmdb environment
            path
            -- maxreaders, I chose this arbitrarily.
            -- The vcache package doesn't set it, so I'll comment it out for now
            -- mdb_env_set_maxreaders env 10000
            Nothing -- 10000
            -- LDMB is designed to support a small handful of databases.
            -- i choose 12 (arbitarily) as maxdbs
            -- The vcache package uses 5
            (Just 12)
            -- LMDB Environment flags
            []

        g <- makeGen mbnoise
        gv <- newMVar g
        return $ DBEnv dbsEnv gv


-- | Close a database.
closeDBEnv :: DBEnv -> IO ()
closeDBEnv (DBEnv (DBS env dir emvar) _) = do
    open <- takeMVar emvar
    if open
        then do
            _ <- mdbTry $ mdb_env_close env
            putMVar emvar False
        else putMVar emvar open


-- | Run a single atomic transaction on an open database.  If something goes
-- wrong, an 'LMDB_Error' value will be returned.
runDB :: (NFData a, Data a) => DBEnv -> DB a -> IO (Either LMDB_Error a)
runDB env action = tryDB env Left Right action


-- | 'runDB' is implemented using this lower-level interface.  It handles the
-- error and success cases before returning to the caller.  The arguments are
-- as follows:
--
--  [ onError ]   - compute result when transaction fails.
--
--  [ onSuccess ] - compute result on bytestring-evacuated result of
--  transaction.
--
--  [ db_action ] - transaction (with pre-evacuation computations done via
--  fmap).
--
tryDB :: (NFData a, Data a) => DBEnv -> (LMDB_Error -> x) -> (a -> x) -> DB a -> IO x
tryDB (DBEnv (DBS env _ _) gv) onError onSuccess db_action = do
    etxn <- mdbTry $ mdb_txn_begin env Nothing (not $ dbRequiresWrite db_action)
    either (return . onError) (withTxn gv) etxn
 where
    withTxn gv txn = do
        stamp <- if (dbRequiresStamp db_action)
                    then currentTime
                    else return $ error "BUG: unknown transaction time!"
        fin <- newIORef (const $ return ())
        ei <- handle (return . Left)
                     (dbOperation db_action (DBParams { dbtime=stamp, dbRNG=gv }) txn)
        result <- either (return . onError) (fmap onSuccess . copyByteStrings) ei
        either (const $ mdb_txn_abort) (const $ mdb_txn_commit) ei txn
        readIORef fin >>= ($ either Just (const Nothing) ei)
        return result

    copyByteStrings :: ( NFData v
                       , Data v
                       , Applicative f) => v -> f v
    copyByteStrings v = v' `deepseq` pure v'
     where
        v' = Generics.everywhere (Generics.mkT S.copy) v

-- | The 'RNG' type comes in two flavors depending on build settings.  By
-- default, it will be a wrapper on 'Crypto.Random.SystemDRG' or
-- 'Crypto.Random.ChaChaDRG' (supplied by the cryptonite package) depending on
-- whether randomness or repeatable pseudo randomness is selected (the noise
-- file argument to 'openDBEnv').
--
-- If the 'cryptonite' build flag is disabled, however, it is simply an alias
-- for 'Crypto.Random.SystemRNG' (supplied by the crypto-random package).  This
-- type handles both system entropy and pseudo-randomness via a noise file.
#if defined(VERSION_crypto_random)
type RNG = SystemRNG

makeGen :: Maybe FilePath -> IO RNG
makeGen noisefile = do
    pool <- fromMaybe Crypto.Random.createEntropyPool $ do
        path <- noisefile
        Just $ createTestEntropyPool `fmap` S.readFile path
    return (cprgCreate pool :: SystemRNG)

getRandomBytes :: Int -> DB S.ByteString
getRandomBytes n = withRNG (\g -> withRandomBytes g n id)

#else
newtype RNG = RNG (Either SystemDRG ChaChaDRG)
instance DRG RNG where
    randomBytesGenerate n (RNG g) =
        either (second (RNG . Left ) . randomBytesGenerate n)
               (second (RNG . Right) . randomBytesGenerate n) g

makeGen :: Maybe FilePath -> IO RNG
makeGen noisefile = do
    drg <- fromMaybe (Left <$> getSystemDRG) $ do
        path <- noisefile
        Just $ Right . drgNewTest . decodeSeed <$> L.readFile path
    return $ RNG drg
 where
    decodeSeed :: L.ByteString -> (Word64, Word64, Word64, Word64, Word64)
    decodeSeed bs | L.null bs = (0,0,0,0,0)
                  | otherwise = decode $  L.cycle bs

instance MonadRandom DB where
    getRandomBytes n = DB 4 $ \DBParams { dbRNG=rngv } _ -> do
        bs <- modifyMVar rngv (return . swap . randomBytesGenerate n)
        return $ Right bs

#endif

-- | This is only for backward compatibility if the 'Crypto.Random.MonadRandom'
-- class is unavailable (the "cryptonite" build flag was disabled).  It may be
-- used to make use of the 'DBEnv' \'s random generator, which is either system
-- entropy or a pseudo-random algorithm seeded by a noise file.  See
-- 'openDBEnv'.  The internal 'RNG' state will be appropriately mutated.
--
-- Using this, or the 'Crypto.Random.MonadRandom' interface, marks a
-- transaction as requiring entropy.  Entropy is never used internally by this
-- library, but the 'Monad' bind operation will mark a transaction as requiring
-- it.  To avoid that (and to enable read-only transaction when possible),
-- avoid using '>>='.
withRNG :: (RNG -> (a,RNG)) -> DB a
withRNG f = DB 4 $ \DBParams { dbRNG=rngv } _ -> do
    a <- modifyMVar rngv (return . swap . f)
    return $ Right a

-- | Output a debug message.
dbtrace :: String -> DB ()
dbtrace str = DB 0 $ \_ _ -> putStrLn str >> return (Right ())

-- | Output a debug message, without new line.
dbtrac :: String -> DB ()
dbtrac str = DB 0 $ \_ _ -> putStr str >> return (Right ())

-- | Perform an IO action from within a transaction.  Note that this is unsafe
-- and very bad practice. It is provided mainly for temporary hacks and
-- debuging.  This function is effectively 'Control.Monad.IO.Class.liftIO' for
-- the 'DB' monad.
dblift :: IO a -> DB a
dblift action = DB 0 $ \_ _ -> fmap Right action

-- | This fetches multiple key/value pairs from a table created by
-- 'initSingle'.  The given 'S.ByteString' is a common prefix of the serialized
-- keys whose values are of interest.
fetchByPrefix :: forall f k v. (Eq k, Binary k, Binary v) => Single 'BoundedKey k v -> S.ByteString -> DB [(k,v)]
fetchByPrefix (Single dbname dbivar) start  = DB 0 $ \_ txn -> do
    dbi <- performAtomicIO dbivar $ mdb_dbi_open txn Nothing []
    fmap (fmap (map $ decodeStrict *** decodeStrict)) $ getRange txn dbi start

-- | This is the multi-map equivelent of 'fetchByPrefix'.
fetchByPrefixMultiple :: forall f k v. (Eq k, Binary k, Binary v) => Multi 'BoundedKey k v -> S.ByteString -> DB [(k,v)]
fetchByPrefixMultiple (Multi dbname dbivar) start  = DB 0 $ \_ txn -> do
    dbi <- performAtomicIO dbivar $ mdb_dbi_open txn Nothing []
    fmap (fmap (concatMap (\(k,v) -> map ((,) $ decodeStrict k) $ runGet getMany $ L.fromChunks [v])))
        $ getRange txn dbi start


-- | TODO: Don't export this.  This is a helper to fetchByPrefix* functions.
getRange :: MDB_txn -> MDB_dbi -> S.ByteString -> IO (Either LMDB_Error [(S.ByteString,S.ByteString)])
getRange txn dbi start = do
        ei <- handle (return . Left)
                     $ Right <$> mdb_cursor_open txn dbi
        case ei of
            Left e -> return $ Left e
            Right cursor -> do
                -- Hack: ForeignPtr is created just to attach a finalizer to the
                -- cursor.  LMDB docs say it is okay to close the cursor either
                -- before or after the transaction closes, but that the cursor
                -- must be explicitly closed.  Therefore, it should be alright
                -- to leave the timing up to the garbage collector.
                fptr <- mallocForeignPtr :: IO (ForeignPtr Word8)
                Foreign.Concurrent.addForeignPtrFinalizer fptr
                        $ mdb_cursor_close cursor
                unsafeInterleaveIO
                 $ alloca $ \pkey ->
                   alloca $ \pval -> do
                    handle (return . Left) $ do
                        let (fptr,offset,len) = S.toForeignPtr start
                        withForeignPtr fptr $ \ptr -> do
                            let start_mdb = MDB_val (fromIntegral len) (ptr `plusPtr` offset)
                            poke pkey start_mdb
                            xs <- uncons MDB_SET_RANGE (fptr,cursor) pkey pval
                            -- I'm using seq to force the head of the list to
                            -- prevent start_mdb from escaping the
                            -- withForeignPtr scope.  I'm actually not sure if
                            -- this is neccessary.
                            return $ Right $ length (take 1 xs) `seq` xs
 where
    uncons :: MDB_cursor_op
            -> (ForeignPtr Word8, MDB_cursor)
            -> Ptr MDB_val
            -> Ptr MDB_val
            -> IO [(S.ByteString,S.ByteString)]
    uncons cop (ptr,cursor) pkey pval = do
        is_found <- mdb_cursor_get cop cursor pkey pval
        if is_found
            then do
                MDB_val klen kp <- peek pkey
                MDB_val vlen vp <- peek pval
                fkp <- newForeignPtr_ kp
                fvp <- newForeignPtr_ vp
                let key = S.fromForeignPtr fkp 0 (fromIntegral klen)
                    val = S.fromForeignPtr fvp 0 (fromIntegral vlen)
                xs <- unsafeInterleaveIO $ uncons MDB_NEXT (ptr,cursor) pkey pval
                return $ (key,val):xs
            else do
                -- We finalize the ptr to close the cursor.  Because the ptr is
                -- used here and in the other branch of the if statement, we
                -- can be sure that the ptr will not go out of scope before we
                -- are done with the cursor.  If the lazy list is truncated,
                -- the cursor will still be closed eventually due to the
                -- pointer's finalizer.
                finalizeForeignPtr ptr
                return []
