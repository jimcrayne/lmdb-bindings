
import System.Environment (getArgs)
import Database.LMDB
import Database.LMDB.Raw
import Control.Applicative
import qualified Data.ByteString.Char8 as B

main = do
    args <- getArgs
    case args of
        [x] -> main' x
        _  -> putStrLn "Usage: mdblist <path>"
    where
        main' x = do
            tables <- listTables x
            mapM_ B.putStrLn tables


    
