{-# LANGUAGE OverloadedStrings #-}

import Network
import Control.Applicative
import Control.Monad
import Control.Concurrent (forkIO)
import Data.Attoparsec.ByteString as A
import Data.Attoparsec.ByteString.Char8 (endOfLine, space, isSpace_w8, decimal, signed)
import Data.Attoparsec.Enumerator
import Data.ByteString.Char8 as C
import Data.Enumerator as E
import Data.Maybe
import System.IO
import qualified Data.ByteString as B
import qualified Data.Enumerator.List as EL
import qualified Data.Enumerator.Binary as EB
import qualified Data.HashTable.IO as H

type HashTable = H.BasicHashTable Key Value
type Key       = ByteString
type Value     = ByteString

data Command = Get Key | Set Key Value deriving Show

command :: Parser Command
command = (word >>= mkCommand) <* endOfLine where
    mkCommand "get" = Get <$> word
    mkCommand "set" = Set <$> word <* extra <*> value
    mkCommand cmd   = fail $ "invalid command: " ++ C.unpack cmd

    word   = takeWhile1 (not.isSpace_w8) <* optional space
    value  = decimal >>= \len -> endOfLine *> A.take len
    extra  = number >> number >> return ()
    number = signed decimal >> space


serve :: Socket -> HashTable -> IO ()
serve socket table = loop where
    loop = do
        (handle,_,_) <- accept socket
        hSetBuffering handle LineBuffering
        forkIO $ serveClient handle
        loop

    serveClient handle = exec $ commands $$ respond where
        exec i   = run i >>= print
        commands = EB.enumHandle 1024 handle $= E.sequence (iterParser command)
        respond  = EL.concatMapM response =$ EB.iterHandle handle

        response (Get key) = do
            val <- lookup key
            case val of
                Just val -> do
                    let len = C.pack . show . B.length $ val
                    return ["VALUE ", key, " 0 ", len, "\n", val, "\nEND\n"]
                Nothing -> return ["END\n"]
        response (Set key value) = insert key value >> return ["STORED\n"]

    insert = H.insert table
    lookup = H.lookup table

main :: IO ()
main = withSocketsDo $ do
    socket <- listenOn (PortNumber 11211)
    table  <- H.new
    serve socket table
