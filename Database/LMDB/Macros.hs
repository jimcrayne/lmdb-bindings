-- | A database is represented as a collection of mutable references ('DBRef')
-- and two varieties of mutable key/value maps: multi-valued maps ('Multi') and
-- ordinary single-valued maps ('Single').
--
-- Typically, you would declare your refs and maps with a single invocation of the
-- 'database' macro.  For example,
--
-- > database [d|
-- >
-- >    myref = xxx :: DBRef Bool
-- >
-- >    public_bindings = xxx :: Multi 'BoundedKey Nickname RawCertificate
-- >
-- >    freshness = xxx :: Single 'HashedKey RawCertificate Int64
-- >
-- >  |]
--
-- Here, three top-level variables are declared, one ref and two maps.
-- Ordinary Haskell syntax is used to declare database elements, but a special
-- symbol 'xxx' is used as a stand-in to automatically fill in run-time
-- information neccessary for finding this data within the database.  For
-- example, the @myref@ variable will likely have a string "myref" associated
-- with it as a lookup key in the underlying database.
--
-- After these items are declared, database transcations can be specified in a
-- manner very similar to how it is done in the 'Control.Concurrent.STM' monad.
-- A 'DBRef' is analagous to a 'Control.Concurrent.STM.TVar.TVar'.
--
-- Warning: It is currently assumed that 'DBRef', 'Multi', and 'Single' objects
-- will not be used accross multiple LMDB sessions.  If you use
-- 'Database/LMDB.openDBEnv' and 'Database/LMDB.closeDBEnv' repeatedly, then
-- the TVars will have 'Completed' values when they ideally should have been
-- reset to NotStarted so that databases can be re-opened. (FIXME)
{-# LANGUAGE CPP                 #-}
{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE KindSignatures      #-}
{-# LANGUAGE TemplateHaskell     #-}
{-# LANGUAGE ScopedTypeVariables #-}
module Database.LMDB.Macros
    ( database
    , xxx
    , DBRef(..)
    , KeyName
    , MapName
    , DBFlavor(..)
    , FlavorKind
    , flavor
    , Single(..)
    , Multi(..)
    , Pending(..)
    , performAtomicIO
    , tryAtomicIO
    ) where

import Data.Maybe
import Database.LMDB.Raw.Types
import Language.Haskell.TH
import Control.Applicative
import qualified Data.ByteString.Char8 as Char8
import Data.Global.Internal
import qualified Data.ByteString                   as S
import qualified Control.Concurrent.STM as STM
import Control.Concurrent.STM.TVar
import Data.Typeable
import Debug.Trace
import Control.Exception

-- | Kind for selecting LMDB-based lookup-table implementation.
--
-- It is intended that this is supplied as an argument to the 'Single' and
-- 'Multi' types.  You will likely need to enable @DataKinds@.
data DBFlavor
    = HashedKey -- ^ keys are serialized and hashed before used as lookups, collisions are handled
    | BoundedKey -- ^ keys are limited to a hardcoded maximum (probably 511 bytes, see LMDB's max_key_size() )
    | BoundedKeyValue -- ^ both keys and values are limited to hardcoded maximum
                      -- (probably 511 bytes, see LMDB's max_key_size() )

#if !MIN_VERSION_base(4,7,0)
data Proxy (f :: DBFlavor) = Proxy
#endif

type MapName = String
type KeyName = S.ByteString


catchAny :: IO a -> (SomeException -> IO a) -> IO a
catchAny = Control.Exception.catch

-- | This type is used to represent the state of a resource that must be
-- initialized on first-use but which might be used in multiple threads and
-- should not be initialized more than once.
--
-- In this library, it is used to track open LMDB tables.
data Pending a
    = NotStarted  -- ^ The resource is not initialized.
    | Pending     -- ^ Initialization is in progress.
    | Completed a -- ^ The fully-initialized value.

-- | Obtain a 'Completed' value.  If it was not initialized and not 'Pending'
-- in any other thread, then initialization will occur in this thread.
performAtomicIO :: TVar (Pending a) -> IO a -> IO a
performAtomicIO var action = do
    getdatum <- STM.atomically $ do
        progress <- readTVar var
        case progress of
            NotStarted -> do
                writeTVar var Pending
                return $ do
                    catchAny 
                          (do datum <- action
                              STM.atomically $ writeTVar var (Completed datum)
                              return datum)
                          (\e -> do STM.atomically $ writeTVar var NotStarted
                                    throw e)
            Pending -> STM.retry
            Completed datum -> return $ return datum
    getdatum


-- | Like 'performAtomicIO' except initialization failure is allowed.  In the
-- case of failure, the variable is left in the 'NotStarted' state so that the
-- action may be retried.  Failure is indicated by the 'Left' result.
tryAtomicIO :: TVar (Pending a) -> IO (Either e a) -> IO (Either e a)
tryAtomicIO var action = do
    getdatum <- STM.atomically $ do
        progress <- readTVar var
        case progress of
            NotStarted -> do
                writeTVar var Pending
                return $ do
                    putStrLn $ "pending tryAtomicIO ..."
                    edatum <- action
                    putStrLn $ "setting TVar..."
                    case edatum of
                        Right x -> STM.atomically $ writeTVar var (Completed x)
                        Left _  -> STM.atomically $ writeTVar var NotStarted
                    putStrLn $ "... completed or not-started tryAtomicIO"
                    return edatum
            Pending -> STM.retry
            Completed datum -> return $ return (Right datum)
    getdatum




-- | A persistent mutable reference that refers to a value within an LMDB
-- database.  Although the constructor is exported, it is preferred that you
-- use the 'database' macro to instantiate this type.
data DBRef t = DBRef KeyName (TVar (Pending MDB_dbi))

-- | A persistent lookup-table implemented as a table within an LMDB database.
-- Although the constructor is exported, it is preferred that you use the
-- 'database' macro to instantiate this type.
data Single (f :: DBFlavor) k v = Single MapName (TVar (Pending MDB_dbi))

-- | A persistent lookup-table implemented as a table within an LMDB database.
-- This is similar to 'Single' except that multiple values may be associated
-- with a single key.  Although the constructor is exported, it is preferred
-- that you use the 'database' macro to instantiate this type.
--
-- The current implementation has no advantage over 'Single' unless the
-- 'BoundedKeyValue' flavor is used.  In that case, the table will be
-- implemented as a DUPSORT table in the LMDB database.
data Multi (f :: DBFlavor) k v  = Multi MapName (TVar (Pending MDB_dbi))


-- | Use this interface to resolve a type-level 'DBFlavor' into a run-time
-- value.
class FlavorKind (f :: DBFlavor)     where flavor :: Proxy f -> DBFlavor
instance FlavorKind 'HashedKey       where flavor Proxy = HashedKey
instance FlavorKind 'BoundedKey      where flavor Proxy = BoundedKey
instance FlavorKind 'BoundedKeyValue where flavor Proxy = BoundedKeyValue

tblFlavor :: forall m f k v. FlavorKind f => m f k v -> DBFlavor
tblFlavor _ = flavor (Proxy :: Proxy f)


{-
dbref :: String -> Q Type -> Q [Dec]
dbref name typ = declareRef ''DBRef mkref name typ
 where
    mkref = [| DBRef (Char8.pack name) <$> newTVarIO NotStarted |]

dbmap :: String -> Q Type -> Q [Dec]
dbmap name typ = declare typ mkref name
 where
    isMulti (AppT c _) = isMulti c
    isMulti t = t == ConT ''Multi

    mkref = do
        t <- typ
        if isMulti t then [| Multi  (Char8.pack name) <$> newTVarIO NotStarted |]
                     else [| Single (Char8.pack name) <$> newTVarIO NotStarted |]
-}

-- | Place-holder expression designed to be replaced by the 'database' macro.
-- If you use this outside of a 'database' declaration, it will simply expand
-- to a call to 'error'.
xxx :: a
xxx = error "xxx placeholder wasn't replaced by the database macro!"

-- | Use this template-haskell macro to declare 'DBRef', 'Single', and 'Multi'
-- variables.  Assign declared elements, regardless of their type, to 'xxx' in
-- order to have the database layout and other book-keeping data automatically
-- filled in for you.  Database keys will have values that are automatically
-- generated based on your variable names.
database :: Q [Dec] -> Q [Dec]
database qsigs = do
    sigs <- qsigs
    concat <$> sequence (map gen sigs) -- :: Q [Dec]
 where

    gen (SigD name typ) = maybe (return [SigD name' typ]) (flip (declareName typ) name') mkref
     where
        isMulti (AppT c _) = isMulti c
        isMulti t = t == ConT ''Multi

        isSingle (AppT c _) = isSingle c
        isSingle t = t == ConT ''Single

        isDBRef (AppT c _) = isDBRef c
        isDBRef t = t == ConT ''DBRef

        mapname = nameBase name

        name' = mkName mapname

        mkref =
            case typ of
                _ | isMulti  typ -> Just [| Multi  mapname <$> newTVarIO NotStarted |]
                _ | isSingle typ -> Just [| Single mapname <$> newTVarIO NotStarted |]
                _ | isDBRef  typ -> Just [| DBRef  (Char8.pack mapname) <$> newTVarIO NotStarted |]
                _                -> Nothing

    -- alternate: ValD (VarP version2_1627446612)
    --                 (NormalB (SigE (ConE GHC.Tuple.())
    --                                   (AppT (ConT DBRef) (ConT UTF8String))))
    --                 []
    gen (ValD (VarP name) (NormalB (SigE (ConE unit) typ)) []) | unit=='()  = gen (SigD name typ)
    gen (ValD (VarP name) (NormalB (SigE (VarE x)    typ)) []) | x=='xxx    = gen (SigD name typ)

    -- delete: ValD (VarP public_bindings_1627455154) (NormalB (ConE GHC.Tuple.())) []
    gen (ValD _ (NormalB (ConE unit)) []) | unit=='()  = return [] -- delete fake = () bindings
    gen (ValD _ (NormalB (ConE x   )) []) | x=='xxx    = return [] -- delete fake = () bindings

    gen x = trace ("pass-through: "++show x) $ return [x] -- pass through anything else
