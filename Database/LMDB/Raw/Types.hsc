{-# LANGUAGE ForeignFunctionInterface, DeriveDataTypeable #-}
module Database.LMDB.Raw.Types where

#include <lmdb.h>

import Foreign
import Foreign.C


-- | Handle for a database in the environment.
newtype MDB_dbi = MDB_dbi { _dbi :: MDB_dbi_t }

type MDB_dbi_t = #type MDB_dbi

