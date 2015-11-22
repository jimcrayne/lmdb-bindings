
module KVStore.LMDB
    ( DBS
    , withDBSDo
    , initDBS
    , shutDownDBS 
    , createDB
    , openDB
    , closeDB
    , fetch
    , store
    , dumpToList
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


data DBS = DBS { dbEnv :: MDB_env
               , dbDir :: FilePath
               }
data DBHandle = DBH { dbhEnv :: MDB_env
                    , dbhDBI :: MDB_dbi 
                    , compiledWriteFlags :: MDB_WriteFlags}

withDBSDo :: FilePath -> (DBS -> IO a) -> IO a
withDBSDo dir action = bracket (initDBS dir) shutDownDBS action

foreign import ccall getPageSizeKV :: CULong

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
    return (DBS env dir)

shutDownDBS :: DBS -> IO ()
shutDownDBS (DBS env _)= mdb_env_close env

internalOpenDB :: [MDB_DbFlag] -> DBS -> ByteString -> IO DBHandle
internalOpenDB flags (DBS env _) name = do 
    txn <- mdb_txn_begin env Nothing False
    db <- mdb_dbi_open txn (Just (S.unpack name)) flags
    mdb_txn_commit txn
    return $ DBH env db (compileWriteFlags [])

createDB ::  DBS -> ByteString -> IO DBHandle
createDB = internalOpenDB [MDB_CREATE]

openDB ::  DBS -> ByteString -> IO DBHandle
openDB = internalOpenDB []

closeDB :: DBHandle -> IO ()
closeDB (DBH env dbi writeflags) = mdb_dbi_close env dbi


fetch :: DBHandle -> ByteString -> IO (Maybe ByteString)
fetch (DBH env dbi _) key = do
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
            let commitTransaction = do -- putStrLn "Finalizing (fetch)" 
                                       mdb_txn_commit txn
            addForeignPtrFinalizer fptr commitTransaction 
            mdb_txn_commit txn
            return . Just $ fromForeignPtr fptr 0  (fromIntegral size)

store :: DBHandle -> ByteString -> ByteString -> IO Bool
store (DBH env dbi writeflags) key val = do
    txn <- mdb_txn_begin env Nothing False
    let (kp,koff,klen) = toForeignPtr key
        (vp,voff,vlen) = toForeignPtr val
    b <- withForeignPtr kp $ \kptr -> withForeignPtr vp $ \vptr -> do
            let key' = MDB_val (fromIntegral klen) (kptr `plusPtr` koff)
                val' = MDB_val (fromIntegral vlen) (vptr `plusPtr` voff)
            mdb_put writeflags txn dbi key' val'
    mdb_txn_commit txn
    return b

dumpToList :: DBHandle -> IO [(ByteString, ByteString)]
dumpToList (DBH env dbi _) = do
    txn <- mdb_txn_begin env Nothing False
    cursor <- mdb_cursor_open txn dbi
    ref <- newMVar 0
    let finalizer = do
            modifyMVar_ ref (return . subtract 1) -- (\x -> (x,x-1)) 
            r <- readMVar ref
            when (r <= 0) $ do
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
    mdb_txn_commit txn
    return $ concatMap fst xs

