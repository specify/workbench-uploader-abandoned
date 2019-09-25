module Main where

import Prelude

import Data.Array (length)
import Data.Maybe (Maybe(..))
import Data.Newtype (wrap)
import Data.Traversable (for_)
import Effect (Effect)
import Effect.Aff (launchAff_)
import Effect.Class (liftEffect)
import Effect.Console (log, logShow)
import Foreign (Foreign)
import MatchRows (insertNewVals, matchRows, selectForInsert)
import MySQL.Connection (closeConnection, createConnection, defaultConnectionInfo, execute_, query_)
import MySQL.Transaction as T
import Simple.JSON (write, writeJSON)
import UploadPlan (ColumnType(..), MappingItem, UploadTable)

main :: Effect Unit
main = do
  -- logShow matchRows_

  conn <- createConnection $ defaultConnectionInfo {database = "uconnverts", password = "Master", user = "Master"}
  launchAff_ do
    T.begin conn
    matchedRows :: Array {localityid :: Int, rownumber :: Int} <- query_ (show $ matchRows uploadTable) conn


    let insert = insertNewVals uploadTable $ map (_.rownumber) matchedRows
    liftEffect $ log insert

    execute_ insert conn

    matchedRows :: Array {localityid :: Int, rownumber :: Int} <- query_ (show $ matchRows uploadTable) conn
    liftEffect $ logShow $ length matchedRows

    T.rollback conn
    liftEffect $ closeConnection conn

uploadTable :: UploadTable
uploadTable =
  { workbenchId: 27
  , tableName: "locality"
  , idColumn: "localityid"
  , filters: [{columnName: "disciplineid", value: "3"}]
  , mappingItems:
    [ {columnName: "shortName", columnType: StringType, id: 1907 }
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
  , staticValues:
    [ {columnName: "srclatlongunit", value: "0"}
    , {columnName: "disciplineid", value: "3"}
    , {columnName: "timestampcreated", value: "now()"}
    ]
  }

