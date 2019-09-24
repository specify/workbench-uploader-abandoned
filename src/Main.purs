module Main where

import Prelude

import Data.Maybe (Maybe(..))
import Data.Newtype (wrap)
import Data.Traversable (for_)
import Effect (Effect)
import Effect.Aff (launchAff_)
import Effect.Class (liftEffect)
import Effect.Console (log, logShow)
import Foreign (Foreign)
import MatchRows (matchRows, selectForInsert)
import MySQL.Connection (closeConnection, createConnection, defaultConnectionInfo, query_)
import MySQL.Transaction as T
import SQL (SelectExpr(..), SelectTerm(..), equal, query, queryDistinct, (..))
import Simple.JSON (write, writeJSON)
import UploadPlan (ColumnType(..), MappingItem)

main :: Effect Unit
main = do
  logShow matchRows_

  conn <- createConnection $ defaultConnectionInfo {database = "uconnverts", password = "Master", user = "Master"}
  launchAff_ do
    T.begin conn
    matchedRows :: Array {localityid :: Int, rownumber :: Int} <- query_ (show matchRows_) conn

    let sfi = selectForInsert_ $ map (_.rownumber) matchedRows
    liftEffect $ logShow $ sfi

    (result2 :: Array Foreign) <- query_ (show sfi ) conn
    for_ result2 \r -> do
      liftEffect $ log $ writeJSON r
    T.rollback conn
    liftEffect $ closeConnection conn


mappingItems :: Array MappingItem
mappingItems = [ {columnName: "shortName", columnType: StringType, id: 1907 }
               , {columnName: "localityName", columnType: StringType, id: 1908 }
               , {columnName: "Text2", columnType: StringType, id: 1913 }
               , {columnName: "verbatimElevation", columnType: StringType, id: 1930 }
               , {columnName: "minElevation", columnType: DoubleType, id: 1931 }
               , {columnName: "maxElevation", columnType: DoubleType, id: 1932 }
               , {columnName: "verbatimLatitude", columnType: StringType, id: 1938 }
               , {columnName: "latitude1", columnType: DecimalType, id: 1939 }
               , {columnName: "verbatimLongitude", columnType: StringType, id: 1940 }
               , {columnName: "longitude1", columnType: DecimalType, id: 1941 }
               ]

matchRows_ :: SelectExpr
matchRows_ = matchRows 27 mappingItems (Table "locality") (\t -> Just $ (t .. "disciplineid") `equal` wrap "3") "localityid"

selectForInsert_ :: Array Int -> SelectExpr
selectForInsert_ =
  selectForInsert 27 mappingItems
  [SelectAs "srclatlongunit" $ wrap "0", SelectAs "disciplineid" $ wrap "3", SelectAs "timestamp" $ wrap "now()" ]

