{-# LANGUAGE
      DeriveAnyClass 
    , StandaloneDeriving
    , TypeFamilies
    , UndecidableInstances 
    , PartialTypeSignatures
    , ImpredicativeTypes
    , DuplicateRecordFields
    #-}

module Nostr.Beam where

import Prelude as P
import Database.Beam as B
import Database.Beam.Sqlite
import Database.SQLite.Simple as SQL
import Database.Beam.Migrate.Simple
import Database.Beam.Backend.SQL.BeamExtensions
import Database.Beam.Sqlite.Migrate (migrationBackend)
import Data.Time
import Data.Text as T
import Data.List as L 
import Data.Maybe
import qualified Data.ByteString as BS 
import Nostr.Event
import Nostr.Filter
import Nostr.Keys
import Nostr.Kinds
import Data.Aeson
import Data.Int
import Nostr.Db
import Control.Monad.State
import Control.Monad.STM
import Control.Exception
import Control.Concurrent.STM.TChan
import Database.SQLite.Simple.Function as SQL
import Crypto.Hash.SHA256 as SHA256
import Data.ByteString.Char8 as BS8
import Data.Time.Clock.POSIX
import Data.Foldable as F
import Text.URI

type WriteDb = TChan (IO ())

type WriteReadDb = (WriteDb, Connection)

insertLoop :: WriteDb -> IO () 
insertLoop tc = forever do 
    ins <- atomically (readTChan tc) 
    ins

createDb :: Connection -> IO (TChan Event) 
createDb o = do 
    _ <- runBeamSqlite o $ do 
        veri <- verifySchema migrationBackend spec
        _ <- checkSchema migrationBackend spec mempty
        case veri of 
            VerificationFailed _ -> autoMigrate migrationBackend spec
            VerificationSucceeded -> pure () 
        veri2 <- verifySchema migrationBackend spec
        case veri2 of 
            VerificationFailed e -> error . show $ e
            VerificationSucceeded -> pure () 
        
    _ <- execute_ o "CREATE TRIGGER IF NOT EXISTS updaterhook AFTER INSERT ON events BEGIN SELECT eventfeed(NEW.con); END;"
    chan <- newTChanIO
    _ <- createFunction o "eventfeed" (eventfeed chan) 
    pure chan


dbIdentity :: Connection -> IO Hex96    
dbIdentity o = do  
    idents <- getIdentities o
    case idents of 
        [] -> genKeyPair >>= (\me -> (insertId o me) >> pure me)
        me : _ -> pure me

        
eventfeed :: TChan Event -> Text -> IO Text  
eventfeed chan t = case qw t of 
    Just e -> atomically $ writeTChan chan e
    _ -> pure () 
    >> pure "1"

data InsMode = Regular | Replace | ParReplace | Delete deriving (Show)

insMode :: Event -> InsMode 
insMode e = case kind . con $ e of
    0 -> Replace 
    5 -> Delete
    ((<10000) -> True) -> Regular
    ((<20000) -> True) -> Replace
    ((<30000) -> True) -> Regular 
    ((<40000) -> True) -> ParReplace 
    _ -> Regular

removeEv :: Text -> SqliteM ()
removeEv ee' = do 
    runDelete $ B.delete (_azt spec') 
                (\a -> (val_ (EvId ee') ==.) . _iieid $ a) 
    runDelete $ B.delete (_mentions spec')
                (\a -> (val_ (EvId ee') ==.) . _eidm $ a) 
    runDelete $ B.delete (_replies spec')
                (\a -> (val_ (EvId ee') ==.) . _eidr $ a) 
    runDelete $ B.delete (_events spec') 
                (\a -> (val_ ee' ==.) . _eid $ a) 


insertEv :: Connection -> Event -> IO (Either SQLError ())
insertEv conn e@(Event i _ (Content{..})) = do 
    ex <- calcExpiry e
    try . runBeamSqliteDebug print conn $ do
        runInsert $ insertOnConflict (_plebs spec') 
                                     (insertExpressions [Pleb (val_ $ wq pubkey)])
                                     anyConflict
                                     onConflictDoNothing
        case insMode e of 
            Regular -> pure ()  
            Delete -> forM_ tags \case 
                ETag (wq -> ee) _ _ -> retireByEid pubkey ee
                ATag (ARef k _ md) _ -> case md of 
                    Nothing -> runUpdate $ B.update (_events spec')
                        (\e' -> _expires e' <-. val_ (mxpiry 1337) ) 
                        (\e' ->     _kind e' ==. val_ (fromIntegral k)
                                &&. (PlebId (val_ $ wq pubkey) ==. _pub e')
                        )
                    Just d -> do 
                        me <- replLook k pubkey (AZTag 'd' d)  
                        for_ me (retireByEid pubkey)
                _ -> pure () 
            Replace -> do 
                e' :: Maybe Text <- runSelectReturningOne $ select do
                    ee <- all_ (_events spec')
                    guard_ $ _pub ee ==. (val_ . PlebId . wq $ pubkey)
                    guard_ $ _kind ee ==. (val_ . (fromIntegral :: Int -> Int32) $ kind)
                    pure . _eid $ ee
                for_ e' removeEv 
            ParReplace -> do 
                let mfirstD = F.find isD $ tags  
                e' <- case mfirstD of 
                    Just z -> replLook kind pubkey z
                    _ -> pure Nothing 
                for_ e' removeEv
        runInsert $ B.insert (_events spec') (insertValues [toEv ex e])
        mapM_ insertTz $ tags <> getContentTags e 
    where 
    isD :: Tag -> Bool
    isD (AZTag 'd' _) = True
    isD _ = False
    insertTz :: Tag -> SqliteM ()
    insertTz = \case  
        ETag ie _ marker -> into (_replies spec') [reply ie marker] 
        PTag ip _ _ -> into (_mentions spec') [mention ip] 
        AZTag c t -> 
            let azid :: ByteString
                azid = SHA256.hash  . BS.toStrict  . encode  $ (c,t)
            in do 
            into (_azt spec') [AzRef default_ 
                                   (val_ azid) 
                                   (val_ . EvId . wq $ i)
                              ] 

        _ -> pure ()  
        
    reply :: Hex32 -> Maybe Marker -> ReplyT (QExpr Sqlite m) 
    reply id' marker = 
        Reply default_ (val_ . EvId . wq $ i) (val_ . wq $ id') (val_ marker)
    
    mention :: Hex32 -> MentionT (QExpr Sqlite m) 
    mention id' = Mention default_ (val_ . EvId . wq $ i) (val_ . wq $ id') 

retireByEid :: Hex32 -> Text -> SqliteM () 
retireByEid pubkey ee = runUpdate $ B.update (_events spec') 
    (\e' -> _expires e' <-. val_ (mxpiry 1337) ) 
    (\e' -> (&&.)  
            (PlebId (val_ $ wq pubkey) ==. _pub e')
            (val_ ee ==. _eid e')
    )    

replLook :: Int -> Hex32 -> Tag -> SqliteM (Maybe Text) 
replLook kind pubkey (AZTag c t) =  
    runSelectReturningOne $ select do
        ee <- all_ (_events spec')
        let azid = SHA256.hash  . BS.toStrict  . encode  $ (c,t)
        ref <- filter_ 
                   (\rf ->  val_ azid ==. _azref rf) 
                   (all_ (_azt spec'))
        guard_ (_iieid ref `references_` ee)
        guard_ $ _pub ee ==. (val_ . PlebId . wq $ pubkey)
        guard_ $ _kind ee ==. (val_ . (fromIntegral :: Int -> Int32) $ kind)
        pure . _eid $ ee
replLook _ _ _ = pure Nothing 

into b = runInsert . B.insert b . insertExpressions

toEv :: Maybe LocalTime -> Event -> EvT Identity 
toEv x e = Ev 
      (wq $ eid e) 
      (PlebId . wq . pubkey . con $ e) 
      (etime . fromInteger . created_at . con $ e) 
      (fromIntegral . kind . con $ e)
      x
      (wq e)

mxpiry :: Int64 -> Maybe LocalTime
mxpiry = Just . etime 

etime :: Int64 -> LocalTime
etime = zonedTimeToLocalTime  . utcToZonedTime utc 
              . posixSecondsToUTCTime . realToFrac 

calcExpiry :: Event -> IO (Maybe LocalTime)
calcExpiry e = case L.filter isExp . tags . con $ e of 
    (Expiry x) : _ -> pure . mxpiry $ x 
    _ -> case kind . con $ e of 
        (isEphemeral -> True) -> do 
            zo <- getCurrentTimeZone
            ti <- addUTCTime fifteen <$> getCurrentTime
            let ow = zonedTimeToLocalTime $ utcToZonedTime zo ti
            pure . Just $ ow
        _ -> pure Nothing
    where 
    isExp (Expiry{}) = True
    isExp _ = False  
    isEphemeral k = k >= 20000 && k < 30000
    
fifteen :: NominalDiffTime
fifteen = secondsToNominalDiffTime . realToFrac $ 15 * 60 

insertId :: Connection -> Hex96 -> IO ()
insertId conn privKey = runBeamSqlite conn $
    runInsert $ B.insert (_identities spec') 
              $ insertValues [Id $ wq privKey]

getIdentities :: Connection -> IO [Hex96]
getIdentities conn = runBeamSqlite conn $ do 
    idz <- runSelectReturningList . select $ all_ (_identities spec')
    pure $ mapMaybe (qw . _priv) idz
    
insertOrigin :: Connection -> URI -> Hex32 -> IO () 
insertOrigin db uri eid = runBeamSqlite db $ do 
    runInsert $ insertOnConflict (_relays spec') 
                                 (insertValues [ Relay . render $ uri ])
                                 anyConflict
                                 onConflictDoNothing
    runInsert $ B.insert (_origins spec') 
              $ insertExpressions 
              [Origin default_ 
                      (val_ . RelayId . render $ uri) 
                      (val_ . EvId . wq $ eid ) ]

isHex32 :: Text -> Bool 
isHex32 h = case decode . encode $ h of  
    (Just (Hex32 _)) -> True
    _ -> False

toHex32 :: Text -> Maybe Hex32
toHex32 = decode . encode 

fetchx :: SQL.Connection -> [Filter] -> IO [Event]
fetchx db fx = nub . mconcat <$> mapM (fetch db) fx 

fetch :: SQL.Connection -> Filter -> IO [Event]

fetch _ (Filter _ _ _ _ _ _ _ _ (Just (Limit 0))) = return []

fetch db (Filter (Just (Ids tx@(P.all isHex32 -> True))) _ _ _ _ _ _ _ _ ) = do 
    hx <- mapM (lookupEid db) $ mapMaybe toHex32 tx
    pure . mapMaybe (qw . _con) . catMaybes $ hx
        
fetch db ff@Filter{..} =  
    mapMaybe (qw . _con) <$> runBeamSqlite db (s' d') 
    where 
    -- s' :: _ -- Q Sqlite db x y 
    s' = case limitF of 
       Just (Limit (fromIntegral -> x)) 
            -> runSelectReturningList . select 
                                      . limit_ x 
                                      . nub_ 
                                      . orderBy_ (desc_ . _time) 

       _ -> runSelectReturningList . select 
                                   . limit_ 10000000 
                                   . nub_ 
                                   . orderBy_ (desc_ . _time) 
        
    d' = getQf ff  
  
getQf :: Filter -> Q Sqlite Db s (EvT (QExpr Sqlite s))
getQf Filter{..} = 
    do  e <- all_ (_events spec')

        guard_ $ fromMaybe_ currentTimestamp_ (_expires e) >=. currentTimestamp_ 

        guard_ $ _time e <=. currentTimestamp_
        
        case idsF of 
            Just (Ids (P.map (val_ . (<>"%")) -> px)) -> 
                guard_ $ P.foldr ((||.) . like_ (_eid e)) 
                                 (val_ False) px 
            _ -> pure () 
        
        case authorsF of 
            Just (Authors (P.map (val_ . (<> "%")) -> px)) -> 
                guard_ $ P.foldr ((||.) . like_ ((\(PlebId p) -> p) $ _pub e)) 
                                 (val_ False) px
            _ -> pure () 
        
        case kindsF of 
            Just (Kinds (P.map fromIntegral -> kx)) -> 
                guard_ (in_ (_kind e) kx)
            _ -> pure () 
        
        case etagF of 
            Just (ETagM (P.map (val_ . wq) -> ex)) -> do  
                ref <- filter_ (\rep -> in_ (_eidrr rep) ex) 
                               (all_ (_replies spec'))
                guard_ (_eidr ref `references_` e)
            _ -> pure ()

        case ptagF of 
            Just (PTagM (P.map (val_ . wq) -> px)) -> do 
                ref <- filter_ (\men -> in_ (_pidm men) px)
                               (all_ $ _mentions spec')
                guard_ (_eidm ref `references_` e)
            _ -> pure () 

        case sinceF of 
            Just (Since (etime . fromIntegral -> s)) -> 
                guard_ $ (_time e >. val_ s )
            _ -> pure ()

        case untilF of 
            Just (Until (etime . fromIntegral -> u)) -> 
                guard_ $ (_time e <. val_ u )
            _ -> pure () 

        forM_ aztagF \case 
            AZTag c t -> do  
                let azid = SHA256.hash  . BS.toStrict  . encode  $ (c,t)
                ref <- filter_ (\rf -> val_ azid ==. _azref rf) (all_ (_azt spec'))
                guard_ (_iieid ref `references_` e)
            _ -> pure ()
            
        pure e

countFx :: SQL.Connection -> Filter -> IO [Int32]
countFx db ff = runBeamSqlite db 
    $ runSelectReturningList . select 
    $ aggregate_ (\_ -> as_ @Int32 countAll_) 
    $ nub_ 
    $ getQf ff


popularityContest :: SQL.Connection -> IO _
popularityContest db = runBeamSqlite db (s d) 
    where 
    -- s :: _
    s = runSelectReturningList . select . limit_ 5 . orderBy_ (desc_ . snd) 
    -- d :: _
    d = do 
        aggregate_ 
            (\p -> (group_ (_pidm p), as_ @Int32 countAll_) ) 
            (all_ . _mentions $ spec')            
            

activityContest :: SQL.Connection -> IO _
activityContest db = runBeamSqlite db $ s d 
    where 
    s = runSelectReturningList . select . limit_ 5 . orderBy_ (desc_ . snd) 
    d = do 
        aggregate_ 
            (\p -> (group_ (_eidrr p), as_ @Int32 countAll_) ) 
            (all_ . _replies $ spec')            


lookupPid :: SQL.Connection -> Text -> IO [Text] 
lookupPid db t = P.map (content . con) <$> fetch db emptyF{authorsF=Just aye}
    where 
    aye = Authors [t]
    
lookupEid :: SQL.Connection -> Hex32 -> IO _ 
lookupEid db t = 
    runBeamSqlite db 
    $ runSelectReturningOne 
    $ lookup_ (_events spec') (EvId . wq $ t)


    

