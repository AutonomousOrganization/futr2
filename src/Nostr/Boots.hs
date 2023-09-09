{-# LANGUAGE QuasiQuotes #-}
module Nostr.Boots where 

import Text.URI (URI)
import Text.URI.QQ (uri)

defaultRelay :: [URI] 
defaultRelay =  
    [ [uri|ws://cvpcawhvxk27qvu5xrcblx7ingfxfivukesdpj7rwg63mflaly3tbuid.onion|]
    , [uri|wss://relay.nostr.info|]
    , [uri|wss://relay.snort.social|]
    , [uri|wss://nostr-pub.wellorder.net|]
    , [uri|wss://nostr.oxtr.dev|]
    , [uri|wss://brb.io|]
    , [uri|wss://nostr-pub.semisol.dev|]
    , [uri|wss://nostr.zebedee.cloud|]
    , [uri|wss://relay.stoner.com|]
    , [uri|wss://relay.nostr.bg|]
    , [uri|wss://nostr-relay.untethr.me|]
    , [uri|wss://nostr.wine|]
    , [uri|wss://nostr.sandwich.farm|]
    , [uri|wss://nostr.rocks|] 
    , [uri|wss://relay.nostr.com.au|]
    , [uri|wss://nostrja-kari.heguro.com|]
    , [uri|wss://nostrja-kari-nip50.heguro.com|]
    , [uri|wss://purplepag.es|]
    , [uri|wss://nostr.lol|]
    , [uri|wss://nostr.wine|]
    , [uri|wss://relay.kronkltd.net|]
    , [uri|wss://relayable.org|]
    , [uri|wss://lightningrelay.com|]
    , [uri|wss://nostr.wine|]
    , [uri|wss://at.nostrworks.com|]
    , [uri|wss://brb.io|]
    , [uri|wss://btc.klendazu.com|]
    , [uri|wss://deschooling.us|]
    , [uri|wss://knostr.neutrine.com|]
    , [uri|wss://nos.lol|]
    , [uri|wss://nostr-01.bolt.observer|]
    , [uri|wss://nostr-1.nbo.angani.co|]
    , [uri|wss://nostr3.actn.io|]
    , [uri|wss://nostr.actn.io|]
    , [uri|wss://nostr.bch.ninja|]
    , [uri|wss://nostr.bitcoiner.social|]
    , [uri|wss://nostr.bongbong.com|]
    , [uri|wss://nostr.cercatrova.me|]
    , [uri|wss://nostr.easydns.ca|]
    , [uri|wss://nostr.einundzwanzig.space|]
    , [uri|wss://nostrex.fly.dev|]
    , [uri|wss://nostr.fmt.wiz.biz|]
    , [uri|wss://nostr.gruntwerk.org|]
    , [uri|wss://nostr.handyjunky.com|]
    , [uri|wss://nostrich.friendship.tw|]
    , [uri|wss://nostr.kollider.xyz|]
    , [uri|wss://nostr.massmux.com|]
    , [uri|wss://nostr.middling.mydns.jp|]
    , [uri|wss://nostr.mikedilger.com|]
    , [uri|wss://nostr.milou.lol|]
    , [uri|wss://nostr.mom|]
    , [uri|wss://nostr.nodeofsven.com|]
    , [uri|wss://nostr.noones.com|]
    , [uri|wss://nostr.orangepill.dev|]
    , [uri|wss://nostr.oxtr.dev|]
    , [uri|wss://nostr-pub1.southflorida.ninja|]
    , [uri|wss://nostr-pub.semisol.dev|]
    , [uri|wss://nostr-pub.wellorder.net|]
    , [uri|wss://nostr-relay.bitcoin.ninja|]
    , [uri|wss://nostrrelay.com|]
    , [uri|wss://nostr-relay.derekross.me|]
    , [uri|wss://nostr-relay.schnitzel.world|]
    , [uri|wss://nostr.roundrockbitcoiners.com|]
    , [uri|wss://nostr.sandwich.farm|]
    , [uri|wss://nostr.screaminglife.io|]
    , [uri|wss://nostr.semisol.dev|]
    , [uri|wss://nostr.shawnyeager.net|]
    , [uri|wss://nostr.slothy.win|]
    , [uri|wss://nostr.supremestack.xyz|]
    , [uri|wss://nostr.swiss-enigma.ch|]
    , [uri|wss://nostr-verified.wellorder.net|]
    , [uri|wss://nostr-verif.slothy.win|]
    , [uri|wss://nostr.vulpem.com|]
    , [uri|wss://nostr.w3ird.tech|]
    , [uri|wss://nostr.yael.at|]
    , [uri|wss://paid.no.str.cr|]
    , [uri|wss://relay.cryptocculture.com|]
    , [uri|wss://relay.damus.io|]
    , [uri|wss://relay.farscapian.com|]
    , [uri|wss://relay.minds.com/nostr/v1/ws|]
    , [uri|wss://relay.n057r.club|]
    , [uri|wss://relay.nostr.au|]
    , [uri|wss://relay.nostr.band|]
    , [uri|wss://relay.nostr.bg|]
    ]