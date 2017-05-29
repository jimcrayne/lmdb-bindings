{-# LANGUAGE ScopedTypeVariables #-}
module Database.LMDB.Flags where

import Data.Bits
import Data.Word

has flags flag = (flags .&. flag) == flag
notHas flags flag =(not (flags `has` flag))-- (flags .&. flag) /= flag

flagKeyInt64 = flagKeyNum64 .|. flagKeySigned
flagKeyInt32 = flagKeyNum32 .|. flagKeySigned
flagValInt64 = flagValNum64 .|. flagValSigned
flagValInt32 = flagValNum32 .|. flagValSigned
flagKeyWord64 = flagKeyNum64  -- alternate names
flagKeyWord32 = flagKeyNum32  
flagValWord64 = flagValNum64
flagValWord32 = flagValNum32  

flagKeyLengthPrefixed = bit 0 :: Word64
flagValLengthPrefixed = bit 1 :: Word64
flagKeyBinary = bit 2 :: Word64
flagValBinary = bit 3 :: Word64
flagKeyNum64 = bit 4 :: Word64
flagValNum64 = bit 5 :: Word64
flagKeyNum32 = bit 4 :: Word64
flagValNum32 = bit 5 :: Word64
flagKeySigned = bit 6 :: Word64
flagValSigned = bit 7 :: Word64
