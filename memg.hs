{-# LANGUAGE OverloadedStrings #-}

import Network
import Control.Applicative
import Control.Monad
import Control.Concurrent (forkIO)
import Control.Concurrent.ReadWriteLock
import Data.Attoparsec.ByteString as A
import Data.Attoparsec.ByteString.Char8 (isEndOfLine, endOfLine, space, isSpace_w8, decimal, signed)
import Data.ByteString as B
import Data.ByteString.Char8 as C
import Data.Maybe
import Data.Foldable (for_)
import System.IO
import qualified Data.ByteString (ByteString)
import qualified Data.HashTable.IO as H

type HashTable = H.BasicHashTable Key Value
type Key       = ByteString
type Value     = ByteString

data Command = Get Key | Set Key Value deriving Show

command :: Parser Command
command = do
    cmd <- takeWhile1 (not.isSpace_w8) <* space
    case cmd of
        "get" -> Get <$> takeWhile1 (not.isSpace_w8)
            <* space
            <* endOfLine
        "set" -> do
            key <- takeWhile1 (not.isSpace_w8)
                <* space
                <* signed decimal
                <* space
                <* signed decimal
                <* space
            len <- decimal <* endOfLine
            Set key <$> A.take len <* endOfLine
        _ -> fail $ "invalid command: " ++ C.unpack cmd

serve :: Socket -> HashTable -> ReadWriteLock -> IO ()
serve socket table lock = loop where
    loop = do
        (handle,_,_) <- accept socket
        forkIO $ serveClient handle
        loop

    serveClient handle = processCmds "" where
        processCmds = parseWith readChunk command >=> respond

        respond (Done trailing cmd) = do
            case cmd of
                Get key -> withReadLock lock $ do
                    val <- lookup key
                    for_ val $ \val -> do
                        let len = C.pack . show . B.length $ val
                        putStrs ["VALUE ", key, " 0 "]
                        putLns  [len, val]
                    putStrLn "END"
                Set key value -> do
                    withWriteLock lock $ insert key value
                    putStrLn "STORED"
            hFlush handle
            processCmds trailing
        respond _ = return ()

        readChunk = hGetSome handle 1024
        putStr    = C.hPutStr handle
        putStrLn  = C.hPutStrLn handle
        putStrs   = mapM_ putStr
        putLns    = mapM_ putStrLn

    insert = H.insert table
    lookup = H.lookup table

main :: IO ()
main = withSocketsDo $ do
    socket <- listenOn (PortNumber 11211)
    table  <- H.new
    lock   <- newReadWriteLock
    serve socket table lock
