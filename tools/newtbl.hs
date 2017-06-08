{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
import Database.LMDB
import Database.LMDB.Raw
import PackUtf8

import Control.Applicative
import qualified Data.ByteString.Char8 as B
import Data.ByteString.Char8 (ByteString)
import Data.Word
import Foreign.Storable
import Foreign.Ptr
import Foreign.ForeignPtr
import Data.ByteString.Internal
import Foreign.Marshal.Utils
import Control.Exception
import System.Environment

main = do
    args <- map packUtf8 <$> getArgs
    if length args < 2
        then error "Usage: newtbl <ENVIRONMENT> <NAME> <NUM>"
        else do
            let env = unpackUtf8 (args !! 0)
            let lastone = if length args > 2 
                            then case B.readInteger (args !! 2) of
                                    Nothing -> 0
                                    Just (n,"") -> n
                            else 0
            tbls <- flip mapM [0..lastone] $ \_ -> do
                withDBSDo env $ \dbs -> snd <$> newTbl dbs (args !! 1) []
            mapM_ B.putStrLn tbls
