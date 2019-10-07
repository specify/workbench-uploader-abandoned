module MatchRows -- ( matchRows
                 -- , selectForInsert
                 -- , insertNewVals
                 -- , insertForeignKeyMapping
                 -- , selectForeignKeyMapping
                 -- , insertForeignKeyValues
                 -- , deleteColumn
                 -- , deleteMapping
                 -- , MatchedRow
                 -- , script
                 -- ) where
where

import Prelude

import Control.Monad.Writer (Writer, execWriter, tell)
import Data.Array (fromFoldable, intercalate, mapWithIndex, nub, nubBy, uncons)
import Data.Foldable (for_)
import Data.FoldableWithIndex (forWithIndex_)
import Data.Maybe (Maybe(..))
import Data.Newtype (wrap)
import Data.NonEmpty (foldl1, (:|))
import Data.Traversable (for)
import Data.Unfoldable (fromMaybe)
import SQL (Alias(..), JoinExpr, Relation(..), ScalarExpr(..), SelectTerm(..), and, as, equal, from, insertFrom, insertValues, intLiteral, isNull, join, leftJoin, notIn, notInSubQuery, nullIf, or, plus, query, queryDistinct, setUserVar, star, strToDate, stringLiteral, varExpr, (..))
import UploadPlan (ColumnType(..), MappingItem, NamedValue, TemplateId(..), ToOne(..), UploadStrategy(..), UploadTable, WorkbenchId(..), UploadPlan)

type MatchedRow = {recordid :: Int, rownumber :: Int, rowid :: Int}

type Script = Writer (Array String) Unit

runPlan :: UploadPlan -> String
runPlan p =
  intercalate "\n\n" $ map (flip (<>) ";") $ execWriter $ script p

script :: UploadPlan -> Script
script up@{templateId: (TemplateId templateId), workbenchId: (WorkbenchId workbenchId)} = do
  tell ["start transaction"]
  tell ["set @templateid = " <> show templateId]
  tell ["set @workbenchid = " <> show workbenchId]
  insertIdFields up.uploadTable
  doUpload up.uploadTable
  tell [ "rollback" ]

remark :: String -> Script
remark message =
  tell ["select '" <> message <> "' as message"]

doUpload :: UploadTable -> Script
doUpload uploadTable = do
  handleToOnes uploadTable
  handleToManys uploadTable

  let wbTemplateMappingItemId = varExpr $ uploadTable.idColumn

  remark $ "find existing " <> uploadTable.tableName <> " records"
  findExistingRecords wbTemplateMappingItemId uploadTable

  remark $ "insert new " <> uploadTable.tableName <> " records"
  insertNewRecords wbTemplateMappingItemId uploadTable

  remark $ "find newly created " <> uploadTable.tableName <> " records"
  findExistingRecords wbTemplateMappingItemId uploadTable


handleToManys :: UploadTable -> Script
handleToManys ut = do
  for_ ut.toManyTables \{foreignKey, tableName, records} ->
    forWithIndex_ records \index record ->
      for_ record.toOneTables \toOne ->
        handleToManyToOne tableName index toOne

handleToManyToOne :: String -> Int -> ToOne -> Script
handleToManyToOne toManyTable index (ToOne {foreignKey, table}) = do
  handleToOnes table
  handleToManys table

  let wbTemplateMappingItemId = varExpr $ toManyIdColumnVar toManyTable index foreignKey

  remark $ "find existing " <> table.tableName <> " records for " <> toManyTable <> " " <> (show index)
  findExistingRecords wbTemplateMappingItemId table

  remark $ "insert new " <> table.tableName <> " records for " <> toManyTable <> " " <> (show index)
  insertNewRecords wbTemplateMappingItemId table

  remark $ "find newly created " <> table.tableName <> " records for " <> toManyTable <> " " <> (show index)
  findExistingRecords wbTemplateMappingItemId table


handleToOnes :: UploadTable -> Script
handleToOnes ut =
  for_ ut.toOneTables \toOne -> handleToOne ut toOne


handleToOne :: UploadTable -> ToOne -> Script
handleToOne ut (ToOne {foreignKey, table}) = do
  handleToOnes table
  handleToManys table

  remark $ "find existing " <> table.tableName <> " records"
  findExistingRecords wbTemplateMappingItemId table

  remark $ "insert new " <> table.tableName <> " records"
  insertNewRecords wbTemplateMappingItemId table

  remark $ "find newly created " <> table.tableName <> " records"
  findExistingRecords wbTemplateMappingItemId table

  where
    wbTemplateMappingItemId = varExpr $ toOneIdColumnVar ut.tableName foreignKey

insertNewRecords :: ScalarExpr -> UploadTable -> Script
insertNewRecords wbTemplateMappingItemId table = tell $ pure $
  insertFrom
  ( query
    ([star] <> constantVals)
    (flip as (Alias "newvalues") $ queryDistinct
     (mapWithIndex makeSelectWB table.mappingItems)
     (Table "workbenchrow" `as` r)
     (mapWithIndex makeJoinWB table.mappingItems)
     (foldl' and $ [(r .. "workbenchid") `equal` varExpr "workbenchid", excludeMatched])
    )
    []
    Nothing
  )
  columns
  table.tableName
  where
    r = Alias "r"
    constantVals = (\{columnName, value} -> SelectAs columnName $ wrap value) `map` table.staticValues
    columns = map _.columnName table.mappingItems <> map _.columnName table.staticValues
    excludeMatched = (r .. "workbenchrowid") `notInSubQuery` (rowsWithValuesFor wbTemplateMappingItemId)

rowsWithValuesFor :: ScalarExpr -> Relation
rowsWithValuesFor workbenchtemplatemappingitemid =
  query [SelectTerm $ r .. "workbenchrowid"] (Table "workbenchrow" `as` r)
  [join (Table "workbenchdataitem") d Nothing]
  (Just $
   ((d .. "workbenchrowid") `equal` (r .. "workbenchrowid")) `and`
   ((d .. "workbenchtemplatemappingitemid") `equal` workbenchtemplatemappingitemid)
  )
  where
    r = Alias "r"
    d = Alias "d"


findExistingRecords :: ScalarExpr -> UploadTable -> Script
findExistingRecords wbTemplateMappingItemId table = tell $ pure $
  insertFrom
  ( query
    [ SelectAs "rowid" $ wb .. "workbenchrowid"
    , SelectAs "id" $ t .. table.idColumn
    , SelectTerm $ wb .. "rownumber"
    , SelectTerm $ wbTemplateMappingItemId
    ]
    (Table table.tableName `as` t)
    [join (rowsFromWB (varExpr "workbenchid") table.mappingItems) wb (foldl' and $ map compValues table.mappingItems)]
    (foldl' and $
     [ (wb .. "workbenchrowid") `notInSubQuery` (rowsWithValuesFor wbTemplateMappingItemId) ]
     <> (strategyToWhereClause table.strategy t # fromFoldable)
    )
  )
  ["workbenchrowid", "celldata", "rownumber", "workbenchtemplatemappingitemid"]
  "workbenchdataitem"
  where
    t = Alias "t"
    wb = Alias "wb"

strategyToWhereClause :: UploadStrategy -> Alias -> Maybe ScalarExpr
strategyToWhereClause strategy t = case strategy of
  AlwaysCreate -> Nothing
  AlwaysMatch values -> matchAll values
  MatchOrCreate values -> matchAll values
  where
    matchAll values = foldl' and $ map (\{columnName, value} -> (t .. columnName) `equal` wrap value) values

foldl' :: forall a. (a -> a -> a) -> Array a -> Maybe a
foldl' f as = (\{ head, tail } -> foldl1 f (head :| tail)) <$> uncons as

insertIdFields :: UploadTable -> Script
insertIdFields ut = do
  remark "insert an id field for the base table"
  tell [ insertValues [[wrap $ "now()", wrap $ "@templateid", stringLiteral ut.idColumn, stringLiteral ut.tableName]]
         ["timestampcreated", "workbenchtemplateid", "fieldname", "tablename"] "workbenchtemplatemappingitem"
       ]
  tell [ setUserVar ut.idColumn (wrap "last_insert_id()") ]

  insertIdFieldsFromToOnes ut
  insertIdFieldsFromToManys ut

insertIdFieldsFromToOnes :: UploadTable -> Script
insertIdFieldsFromToOnes ut =
  for_ ut.toOneTables \(ToOne {foreignKey, table}) -> do
    remark $ "insert an id field for the " <> ut.tableName <> " to " <> table.tableName <> " foreign key"
    tell [ insertValues
           [[wrap $ "now()", wrap $ "@templateid", stringLiteral foreignKey, stringLiteral ut.tableName]]
           ["timestampcreated", "workbenchtemplateid", "fieldname", "tablename"] "workbenchtemplatemappingitem"
         ]

    tell [ setUserVar (toOneIdColumnVar ut.tableName foreignKey) (wrap "last_insert_id()") ]

    insertIdFieldsFromToOnes table
    insertIdFieldsFromToManys table


toOneIdColumnVar :: String -> String -> String
toOneIdColumnVar tableName foreignKey = tableName <> "_" <> foreignKey

insertIdFieldsFromToManys :: UploadTable -> Script
insertIdFieldsFromToManys ut = do
  for_ ut.toManyTables \{foreignKey, tableName, records} ->
    forWithIndex_ records \index record ->
      for_  record.toOneTables  \(ToOne {foreignKey, table}) -> do
        remark $ "insert an id field for the " <> tableName <> show index <> " to " <> table.tableName <> " foreign key"
        tell [ insertValues
               [[wrap $ "now()", wrap $ "@templateid", stringLiteral foreignKey, stringLiteral tableName]]
               ["timestampcreated", "workbenchtemplateid", "fieldname", "tablename"] "workbenchtemplatemappingitem"
             ]
        tell [ setUserVar (toManyIdColumnVar tableName index foreignKey) (wrap "last_insert_id()") ]

        insertIdFieldsFromToOnes table
        insertIdFieldsFromToManys table


toManyIdColumnVar :: String -> Int -> String -> String
toManyIdColumnVar tableName index foreignKey = tableName <> show index <> "_" <> foreignKey

-- matchRows_ :: WorkbenchId -> Array MappingItem -> Relation -> (Alias -> Maybe ScalarExpr) -> String -> Relation
-- matchRows_ (WorkbenchId wbId) mappingItems matchTable whereExpr idCol =
--   query [SelectAs "rowid" $ wb .. "workbenchrowid", SelectAs "recordid" $ t .. idCol, SelectTerm $ wb .. "rownumber"]
--   (matchTable `as` t) joinWB (whereExpr t)
--   where
--     (t :: Alias) = wrap "t"
--     (wb :: Alias) = wrap "wb"
--     joinWB = case uncons $ map compValues mappingItems of
--       Just { head: c, tail: cs } ->  [join (rowsFromWB wbId mappingItems) wb $ Just (foldl1 and (c :| cs))]
--       Nothing -> []

-- matchRows :: UploadPlan -> Array NamedValue -> Relation
-- matchRows up filters =
--   matchRows_ up.workbenchId up.uploadTable.mappingItems (Table up.uploadTable.tableName) whereClause up.uploadTable.idColumn
--   where whereClause =
--           \t -> map (\{head, tail} -> foldl1 and (head :| tail)) $
--                 uncons $ map (\{columnName, value} -> (t .. columnName) `equal` wrap value)
--                 filters

rowsFromWB :: ScalarExpr -> Array MappingItem -> Relation
rowsFromWB wbId mappingItems =
  query
  (mapWithIndex makeSelectWB mappingItems <> [SelectTerm $ r .. "workbenchrowid", SelectTerm $ r .. "rownumber"])
  (Table "workbenchrow" `as` r)
  (mapWithIndex makeJoinWB mappingItems)
  (Just $ (r .. "workbenchid") `equal` wbId)
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
parseValue (DateType format) value = strToDate value $ stringLiteral format

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

deleteColumn :: MappingItem -> String
deleteColumn mi = "delete from workbenchdataitem where workbenchtemplatemappingitemid = " <> (show mi.id)

deleteMapping :: MappingItem -> String
deleteMapping mi = "delete from workbenchtemplatemappingitem where workbenchtemplatemappingitemid = " <> (show mi.id)
