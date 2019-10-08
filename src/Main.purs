module Main where

import Prelude

import Data.Array (intercalate, length)
import Data.Maybe (Maybe(..))
import Data.Newtype (wrap)
import Data.Traversable (for, for_)
import Debug.Trace (traceM)
import Effect (Effect)
import Effect.Aff (Aff, Error, error, launchAff_, throwError)
import Effect.Class (liftEffect)
import Effect.Console (log, logShow)
import ExamplePlan (uploadPlan)
import Foreign (Foreign)
import MatchRows (runPlan, script)
import MySQL.Connection (Connection, closeConnection, createConnection, defaultConnectionInfo, execute_, query_)
import MySQL.Transaction as T
import SQL (Relation)
import Simple.JSON (class ReadForeign, class WriteForeign, write, writeJSON)


query' :: forall a. ReadForeign a => Connection -> String -> Aff (Array a)
query' conn q = do
  -- liftEffect $ log $ "query: " <> q
  query_ q conn

execute' :: Connection -> String -> Aff Unit
execute' conn s = do
  -- liftEffect $ log $ "execute: " <> s
  execute_ s conn

main :: Effect Unit
main = do
  log $ runPlan uploadPlan



-- foo = do
--   conn <- createConnection $ defaultConnectionInfo {database = "bishop", password = "Master", user = "Master"}
--   launchAff_ do
--     T.begin conn

--     doIt conn

--     T.rollback conn
--     liftEffect $ closeConnection conn

-- doIt :: Connection -> Aff Unit
-- doIt conn = do
--   rowsToIds <- doUploadTable conn uploadPlan
--   pure unit

-- logJSON :: ∀ a. WriteForeign a ⇒ a → Aff Unit
-- logJSON value = liftEffect $ log $ writeJSON value

-- doUploadTable :: Connection -> UploadPlan -> Aff (Array MatchedRow)
-- doUploadTable conn up = do
--   logJSON {uploadingTable: up.uploadTable.tableName}

--   toOnes <- for up.uploadTable.toOneTables \(ToOne {foreignKey, table}) -> do
--     let parentTableName = up.uploadTable.tableName
--     let up' = up { uploadTable = table }
--     doUploadToOne conn up' parentTableName foreignKey

--   -- foo <- for up.uploadTable.toManyTables $ doToManyToOnes conn up
--   -- traceM {name:up.uploadTable.tableName, foo:foo}

--   let up' = up {uploadTable = up.uploadTable {mappingItems = up.uploadTable.mappingItems <> toOnes}}

--   matchedRows <- case up.uploadTable.strategy of
--         AlwaysCreate -> pure []
--         (AlwaysMatch filters) -> findExistingRecords conn up' filters
--         (MatchOrCreate filters) -> findExistingRecords conn up' filters

--   logJSON {insertingRecords: up'.uploadTable.tableName}
--   let insert = insertNewVals up' $ map (_.rownumber) matchedRows
--   liftEffect $ log insert
--   execute' conn insert

--   for_ toOnes \mi -> do
--     execute' conn (deleteColumn mi)
--     execute' conn (deleteMapping mi)

--   findExistingRecords conn up' []

-- findExistingRecords :: Connection -> UploadPlan -> Array NamedValue -> Aff (Array MatchedRow)
-- findExistingRecords conn up filters = do
--   -- liftEffect $ log (show $ matchRows up filters)
--   matchedRows :: Array MatchedRow <- query' conn (show $ matchRows up filters)
--   logJSON {matchedRows: length matchedRows}
--   pure matchedRows

-- doToManyToOnes :: Connection -> UploadPlan -> ToMany -> Aff (Array (Array MappingItem))
-- doToManyToOnes conn parentUploadPlan {foreignKey, tableName, records} = do
--   for records \{toOneTables} -> do
--     for toOneTables \(ToOne {foreignKey, table}) -> do
--       let up = parentUploadPlan { uploadTable = table }
--       doUploadToOne conn up tableName foreignKey
--       -- execute' conn (deleteColumn mi)
--       -- execute' conn (deleteMapping mi)

-- doUploadToOne :: Connection -> UploadPlan -> String -> String -> Aff MappingItem
-- doUploadToOne conn up parentTableName foreignKey = do
--   matchedRows <- doUploadTable conn up

--   let insert = insertForeignKeyMapping up.templateId parentTableName foreignKey
--   execute' conn insert

--   ids :: Array {id :: Int} <- query' conn "select last_insert_id() as id"
--   mappingItemId <- case ids of
--         [{id}] -> pure id
--         otherwise -> throwError $ error $ "failed finding new column mapping: " <> show ids

--   let insert2 = insertForeignKeyValues mappingItemId matchedRows
--   execute' conn insert2

--   pure {columnName: foreignKey, id: mappingItemId, columnType: IntType}
