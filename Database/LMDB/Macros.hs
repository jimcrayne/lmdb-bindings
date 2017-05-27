{-# LANGUAGE CPP                 #-}
{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE KindSignatures      #-}
{-# LANGUAGE TemplateHaskell     #-}
{-# LANGUAGE ScopedTypeVariables #-}
module Database.LMDB.Macros where

import Data.Maybe
import Database.LMDB.Raw.Types
import Language.Haskell.TH
import Control.Applicative
import qualified Data.ByteString.Char8 as Char8
import Data.Global.Internal
import qualified Data.ByteString                   as S
import Control.Concurrent.STM.TVar
import Data.Typeable
import Debug.Trace

-- | Kind for selecting LMDB-based lookup-table implementation.
data DBFlavor
    = HashedKey -- ^ keys are serialized and hashed before used as lookups, collisions are handled
    | BoundedKey -- ^ keys are limited to a hardcoded maximum (probably 511 bytes, see LMDB's max_key_size() )
    | BoundedKeyValue -- ^ both keys and values are limited to hardcoded maximum
                      -- (probably 511 bytes, see LMDB's max_key_size() )

#if !MIN_VERSION_base(4,7,0)
data Proxy (f :: DBFlavor) = Proxy
#endif

type MapName = S.ByteString
type KeyName = S.ByteString

data Pending a = NotStarted | Pending | Completed a

-- Warning: It is currently assumed that 'DBRef', 'Multi', and 'Single' objects
-- will not be used accross multiple LMDB sessions.  If you use openDBEnv and
-- closeDBEnv repeatedly, then the TVars will have 'Completed' values when they
-- ideally should have been reset to NotStarted so that databases can be
-- re-opened. (FIXME)
data DBRef t = DBRef KeyName (TVar (Pending MDB_dbi))

data Multi (f :: DBFlavor) k v  = Multi MapName (TVar (Pending MDB_dbi))

data Single (f :: DBFlavor) k v = Single MapName (TVar (Pending MDB_dbi))

class FlavorKind (f :: DBFlavor)     where flavor :: Proxy f -> DBFlavor
instance FlavorKind 'HashedKey       where flavor Proxy = HashedKey
instance FlavorKind 'BoundedKey      where flavor Proxy = BoundedKey
instance FlavorKind 'BoundedKeyValue where flavor Proxy = BoundedKeyValue

tblFlavor :: forall m f k v. FlavorKind f => m f k v -> DBFlavor
tblFlavor _ = flavor (Proxy :: Proxy f)


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

xxx :: a
xxx = error "xxx placeholder wasn't replaced by the database macro!"

database :: Q [Dec] -> Q [Dec]
database qsigs = do
    sigs <- qsigs
    concat <$> sequence (map gen sigs) -- :: Q [Dec]
 where

    gen (SigD name typ) = maybe (return [SigD name typ]) (flip (declareName typ) name) mkref
     where
        isMulti (AppT c _) = isMulti c
        isMulti t = t == ConT ''Multi

        isSingle (AppT c _) = isSingle c
        isSingle t = t == ConT ''Single

        isDBRef (AppT c _) = isDBRef c
        isDBRef t = t == ConT ''DBRef

        mapname = nameBase name

        mkref =
            case typ of
                _ | isMulti typ -> Just [| Multi  (Char8.pack mapname) <$> newTVarIO NotStarted |]
                _ | isSingle typ -> Just [| Single (Char8.pack mapname) <$> newTVarIO NotStarted |]
                _ | isDBRef typ -> Just [| DBRef (Char8.pack mapname) <$> newTVarIO NotStarted |]
                _ -> Nothing

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
