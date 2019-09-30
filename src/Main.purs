module Main where

import Prelude

import Data.Array (length)
import Data.Maybe (Maybe(..))
import Data.Newtype (wrap)
import Data.Traversable (for, for_)
import Effect (Effect)
import Effect.Aff (Aff, Error, error, launchAff_, throwError)
import Effect.Class (liftEffect)
import Effect.Console (log, logShow)
import Foreign (Foreign)
import MatchRows (MatchedRow, insertForeignKeyMapping, insertForeignKeyValues, insertNewVals, matchRows, selectForInsert, selectForeignKeyMapping)
import MySQL.Connection (Connection, closeConnection, createConnection, defaultConnectionInfo, execute_, query_)
import MySQL.Transaction as T
import SQL (Relation)
import Simple.JSON (class WriteForeign, write, writeJSON)
import UploadPlan (ColumnType(..), MappingItem, TemplateId(..), ToOne(..), UploadPlan, UploadTable, WorkbenchId(..))

main :: Effect Unit
main = do
  conn <- createConnection $ defaultConnectionInfo {database = "uconnverts", password = "Master", user = "Master"}
  launchAff_ do
    T.begin conn

    doIt conn

    T.rollback conn
    liftEffect $ closeConnection conn

doIt :: Connection -> Aff Unit
doIt conn = do
  rowsToIds <- doUploadTable conn uploadPlan
  pure unit

logJSON :: ∀ a. WriteForeign a ⇒ a → Aff Unit
logJSON value = liftEffect $ log $ writeJSON value

doUploadTable :: Connection -> UploadPlan -> Aff (Array MatchedRow)
doUploadTable conn up = do
  logJSON {uploadingTable: up.uploadTable.tableName}

  toOnes <- for up.uploadTable.toOneTables \{foreignKey, table: toOne} ->
    doUploadToOne conn up foreignKey toOne

  let up' = up {uploadTable = up.uploadTable {mappingItems = up.uploadTable.mappingItems <> toOnes}}

  matchedRows :: Array MatchedRow <- query_ (show $ matchRows up') conn

  logJSON {insertingRecords: up'.uploadTable.tableName}
  let insert = insertNewVals up' $ map (_.rownumber) matchedRows
  execute_ insert conn

  query_ (show $ matchRows up') conn

doUploadToOne :: Connection -> UploadPlan -> String -> ToOne -> Aff MappingItem
doUploadToOne conn parentUploadPlan foreignKeyColumn (ToOne ut) = do
  let parentTableName = parentUploadPlan.uploadTable.tableName
  let up = parentUploadPlan { uploadTable = ut }
  matchedRows <- doUploadTable conn up

  let insert = insertForeignKeyMapping up.templateId parentTableName foreignKeyColumn
  execute_ insert conn

  ids :: Array {id :: Int} <- flip query_ conn $ show $ selectForeignKeyMapping up.templateId parentTableName foreignKeyColumn
  mappingItemId <- case ids of
        [{id}] -> pure id
        otherwise -> throwError $ error "failed inserting"

  let insert2 = insertForeignKeyValues mappingItemId matchedRows
  execute_ insert2 conn

  pure {columnName: foreignKeyColumn, id: mappingItemId, columnType: IntType}


uploadPlan :: UploadPlan
uploadPlan =
  { workbenchId: WorkbenchId 27
  , templateId: TemplateId 27
  , uploadTable:
    { tableName: "collectingevent"
    , idColumn: "collectingeventid"
    , filters: [{columnName: "disciplineid", value: "3"}]
    , mappingItems:
      [ {columnName: "verbatimDate", id: 1909, columnType: StringType}
        -- , {columnName: "startDate", id: 1910, columnType: DateType}
      , {columnName: "endDateVerbatim", id: 1911, columnType: StringType}
        -- , {columnName: "endDate", id: 1912, columnType: DateType}
        -- , {columnName: "method", id: 1914, columnType: StringType}
      , {columnName: "stationFieldNumber", id: 1916, columnType: StringType}
      , {columnName: "verbatimLocality", id: 1923, columnType: StringType}
      , {columnName: "remarks", id: 1929, columnType: StringType}
      , {columnName: "text2", id: 1974, columnType: StringType}
      ]
    , staticValues:
      [ { columnName: "disciplineid", value: "3" }
      , { columnName: "timestampcreated", value: "now()" }
      , { columnName: "guid", value: "uuid()" }
      ]
    , toOneTables:
      [ { foreignKey: "localityid",
          table:
          ToOne { tableName: "locality"
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
                  , { columnName: "guid", value: "uuid()" }
                  ]
                , toOneTables: []
                }
        }
      ]
    }
  }
