{-# Language
    DataKinds 
    , OverloadedStrings
#-}

module Main (main) where

import Prelude as P 
import Network.WebSockets as WS
import Control.Monad
import Database.SQLite.Simple as SQL
import Nostr.Beam
import Nostr.Relay
import Nostr.Harvest
import Nostr.Event
import Nostr.Boots
import Control.Concurrent
import Control.Concurrent.STM.TChan
import Control.Monad.STM
import Network.Wai.Handler.Warp
import Servant.API
import Servant.Server
import Data.Proxy
import Data.Text 
import Data.Text.Encoding
import Data.Aeson
import Data.ByteString as BS
import Network.Wai.Handler.WebSockets
import Nostr.Keys
import Nostr.Wire
import Text.URI
import Data.Maybe
import Data.Traversable
import Data.Time.Clock.POSIX
import Nostr.Auth
import Data.Map as M 
import Control.Concurrent.STM.TVar
import Nostr.Pool

main :: IO ()
main = do 
    o <- SQL.open "./futr.sqlite"
    f <- createDb o
    void . forkIO . run 9481 $ websocketsOr co 
        (acceptRequest >=> relay o f) 
        (serve x s) 
    threadDelay 100000

    idents <- getIdentities o
    me' <- case idents of 
        [] -> genKeyPair >>= (\me -> (insertId o . un96 $ me) >> pure me)
        me : _ -> pure me

    tv <- newTVarIO M.empty

    let pool = Pool tv o me'

    mapM_ (addRelay pool) defaultRelay

    sec :: Integer <- round <$> getPOSIXTime
    castAll pool (Subscribe "a" [liveF sec])

    chan' <- atomically . dupTChan $ f
    void . forever $ atomically (readTChan chan') >>= \c -> do   
        print . ("e : "<>) . content . con $ c


type Nip45 = Get '[JSON] Text 
x :: Proxy Nip45
x = Proxy
s :: Server Nip45
s = return . decodeUtf8 . BS.toStrict . encode . object $ 
    [ "name" .= (""::Text) 
    , "description" .= (""::Text) 
    , "pubkey" .= (""::Text) 
    -- XXX configurable 
    ]

co :: ConnectionOptions
co = defaultConnectionOptions