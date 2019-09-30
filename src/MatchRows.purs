module MatchRows ( matchRows
                 , selectForInsert
                 , insertNewVals
                 , insertForeignKeyMapping
                 , selectForeignKeyMapping
                 , insertForeignKeyValues
                 , MatchedRow
                 ) where

import Prelude

import Data.Array (intercalate, mapWithIndex, nub, nubBy, uncons)
import Data.Maybe (Maybe(..))
import Data.Newtype (wrap)
import Data.NonEmpty (foldl1, (:|))
import Data.Unfoldable (fromMaybe)
import SQL (Alias(..), JoinExpr, Relation(..), ScalarExpr(..), SelectTerm(..), and, as, equal, from, insertFrom, insertValues, intLiteral, isNull, join, leftJoin, notIn, nullIf, or, plus, query, queryDistinct, star, stringLiteral, (..))
import UploadPlan (ColumnType(..), MappingItem, TemplateId(..), UploadPlan, UploadTable, WorkbenchId(..))

type MatchedRow = {recordid :: Int, rownumber :: Int, rowid :: Int}

matchRows_ :: WorkbenchId -> Array MappingItem -> Relation -> (Alias -> Maybe ScalarExpr) -> String -> Relation
matchRows_ (WorkbenchId wbId) mappingItems matchTable whereExpr idCol =
  query [SelectAs "rowid" $ wb .. "workbenchrowid", SelectAs "recordid" $ t .. idCol, SelectTerm $ wb .. "rownumber"]
  (matchTable `as` t) joinWB (whereExpr t)
  where
    (t :: Alias) = wrap "t"
    (wb :: Alias) = wrap "wb"
    joinWB = case uncons $ map compValues mappingItems of
      Just { head: c, tail: cs } ->  [join (rowsFromWB wbId mappingItems) wb $ Just (foldl1 and (c :| cs))]
      Nothing -> []

matchRows :: UploadPlan -> Relation
matchRows up = matchRows_ up.workbenchId up.uploadTable.mappingItems (Table up.uploadTable.tableName) whereClause up.uploadTable.idColumn
  where whereClause =
          \t -> map (\{head, tail} -> foldl1 and (head :| tail)) $
                uncons $ map (\{columnName, value} -> (t .. columnName) `equal` wrap value)
                up.uploadTable.filters

rowsFromWB :: Int -> Array MappingItem -> Relation
rowsFromWB wbId mappingItems =
  query
  (mapWithIndex makeSelectWB mappingItems <> [SelectTerm $ r .. "workbenchrowid", SelectTerm $ r .. "rownumber"])
  (Table "workbenchrow" `as` r)
  (mapWithIndex makeJoinWB mappingItems)
  (Just $ (r .. "workbenchid") `equal` (wrap $ show wbId))
  where (r :: Alias) = wrap "r"

compValues :: MappingItem -> ScalarExpr
compValues item = case item.columnType of
  StringType -> (v1 `equal` v2) `or` (isNull v1 `and` isNull v2) `or` (isNull v1 `and` (v2 `equal` wrap "''"))
  otherwise -> (v1 `equal` v2) `or` (isNull v1 `and` isNull v2)
  where v1 = wrap "t" .. item.columnName
        v2 = wrap "wb" .. item.columnName

makeJoinWB :: Int -> MappingItem -> JoinExpr
makeJoinWB i item = leftJoin (Table "workbenchdataitem") dataItem $ Just $
                    ((dataItem .. "workbenchrowid") `equal` (r .. "workbenchrowid")) `and`
                    ((dataItem .. "workbenchtemplatemappingitemid") `equal` (wrap $ show item.id))
  where (dataItem :: Alias) = wrap ("c" <> (show i))
        (r :: Alias) = wrap "r"

parseValue :: ColumnType -> ScalarExpr -> ScalarExpr
parseValue StringType value = value
parseValue DoubleType value = nullIf value (wrap "''") `plus` (wrap "0.0")
parseValue IntType value = nullIf value (wrap "''") `plus` (wrap "0")
parseValue DecimalType value = nullIf value (wrap "''")
parseValue DateType value = nullIf value (wrap "''")

makeSelectWB :: Int -> MappingItem -> SelectTerm
makeSelectWB i item = SelectAs item.columnName (parseValue item.columnType value)
  where value = (wrap $ "c" <> (show i)) .. "celldata"


selectNewVals :: WorkbenchId -> Array MappingItem -> Array Int -> Relation
selectNewVals (WorkbenchId wbId) mappingItems matched =
  queryDistinct
  (mapWithIndex makeSelectWB mappingItems)
  (Table "workbenchrow" `as` r)
  (mapWithIndex makeJoinWB mappingItems)
  (Just $ foldl1 and ((r .. "workbenchid") `equal` (wrap $ show wbId) :| excludeMatched r matched))
  where (r :: Alias) = wrap "r"

selectForInsert :: WorkbenchId -> Array MappingItem -> Array ExtraValue -> Array Int -> Relation
selectForInsert wbId mappingItems extraFields matched =
  query ([star] <> extraFields_)
  (selectNewVals wbId mappingItems matched `as` newValues)
  []
  Nothing
  where (newValues :: Alias) = wrap "newvalues"
        extraFields_ = (\{columnName, value} -> SelectAs columnName $ wrap value) `map` extraFields

excludeMatched :: Alias -> Array Int -> Array ScalarExpr
excludeMatched row matched = fromMaybe $ (row .. "rownumber") `notIn` rowList
  where rowList = map (show >>> wrap) $ nub matched

type ExtraValue = {columnName :: String, value :: String}

insertNewVals_ :: String -> WorkbenchId -> Array MappingItem -> Array ExtraValue -> Array Int -> String
insertNewVals_ table wbId mappingItems extraFields matched =
  insertFrom (selectForInsert wbId mappingItems extraFields matched) columns table
  where columns = map _.columnName mappingItems <> map _.columnName extraFields

insertNewVals :: UploadPlan -> Array Int -> String
insertNewVals up matchedRows =
  insertNewVals_ ut.tableName up.workbenchId ut.mappingItems ut.staticValues matchedRows
  where ut = up.uploadTable


insertForeignKeyMapping :: TemplateId -> String -> String -> String
insertForeignKeyMapping (TemplateId wbTemplateId) tableName columnName =
  insertValues [[wrap "now()", stringLiteral columnName, stringLiteral tableName, intLiteral wbTemplateId]]
  ["timestampcreated", "fieldname", "tablename", "workbenchtemplateid"] "workbenchtemplatemappingitem"


selectForeignKeyMapping :: TemplateId -> String -> String -> Relation
selectForeignKeyMapping wbTemplateId tableName columnName =
  query [SelectAs "id" $ i .. "workbenchtemplatemappingitemid"] (from $ Table "workbenchtemplatemappingitem") [] (Just whereClause)
  where
    i = Alias "workbenchtemplatemappingitem"
    whereClause = ((i .. "fieldname") `equal` stringLiteral columnName) `and` ((i .. "tablename") `equal` stringLiteral tableName)


insertForeignKeyValues :: Int -> Array MatchedRow -> String
insertForeignKeyValues wbTemplateItemId fks =
  insertValues (map rowValues onePerRow) ["workbenchtemplatemappingitemid", "rownumber", "workbenchrowid", "celldata"] "workbenchdataitem"
  where onePerRow = nubBy (\{rowid:a} {rowid:b} -> compare a b) fks
        rowValues fk = [intLiteral wbTemplateItemId, intLiteral fk.rownumber, intLiteral fk.rowid, stringLiteral $ show fk.recordid]
