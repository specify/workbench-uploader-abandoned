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

import Data.Array (fromFoldable, intercalate, mapWithIndex, nub, nubBy, uncons)
import Data.Maybe (Maybe(..))
import Data.Newtype (wrap)
import Data.NonEmpty (foldl1, (:|))
import Data.Traversable (for)
import Data.Unfoldable (fromMaybe)
import SQL (Alias(..), JoinExpr, Relation(..), ScalarExpr(..), SelectTerm(..), and, as, equal, from, insertFrom, insertValues, intLiteral, isNull, join, leftJoin, notIn, notInSubQuery, nullIf, or, plus, query, queryDistinct, setUserVar, star, strToDate, stringLiteral, varExpr, (..))
import UploadPlan (ColumnType(..), MappingItem, NamedValue, TemplateId(..), ToOne(..), UploadPlan, UploadStrategy(..), UploadTable, WorkbenchId(..))

type MatchedRow = {recordid :: Int, rownumber :: Int, rowid :: Int}

remark :: String -> String
remark message =
  "select '" <> message <> "' as message"

script :: UploadPlan -> Array String
script up@{templateId: (TemplateId templateId), workbenchId: (WorkbenchId workbenchId)} =
  [ "start transaction"
  , "set @templateid = " <> show templateId
  , "set @workbenchid = " <> show workbenchId
  ]
  <> insertIdFields up.uploadTable
  <> doUpload up.uploadTable
  <> [ "rollback" ]


doUpload :: UploadTable -> Array String
doUpload uploadTable =
  handleToOnes uploadTable
  <> handleToManys uploadTable
  <> [ remark $ "find existing " <> uploadTable.tableName <> " records"
     , findExistingRecords wbTemplateMappingItemId uploadTable

     , remark $ "insert new " <> uploadTable.tableName <> " records"
     , insertNewRecords wbTemplateMappingItemId uploadTable

     , remark $ "find newly created " <> uploadTable.tableName <> " records"
     , findExistingRecords wbTemplateMappingItemId uploadTable
     ]
  where
    wbTemplateMappingItemId = varExpr $ uploadTable.idColumn

handleToManys :: UploadTable -> Array String
handleToManys ut = do
  {foreignKey, tableName, records} <- ut.toManyTables
  {index, record: {toOneTables}} <- mapWithIndex (\i r ->  {index: i, record: r}) records
  toOne <- toOneTables
  handleToManyToOne tableName index toOne

handleToManyToOne :: String -> Int -> ToOne -> Array String
handleToManyToOne toManyTable index (ToOne {foreignKey, table}) =
  handleToOnes table
  <> handleToManys table
  <> [ remark $ "find existing " <> table.tableName <> " records for "
       <> toManyTable <> " " <> (show index)
     , findExistingRecords wbTemplateMappingItemId table

     , remark $ "insert new " <> table.tableName <> " records for "
       <> toManyTable <> " " <> (show index)
     , insertNewRecords wbTemplateMappingItemId table

     , remark $ "find newly created " <> table.tableName <> " records for "
       <> toManyTable <> " " <> (show index)
     , findExistingRecords wbTemplateMappingItemId table
     ]
  where
    wbTemplateMappingItemId = varExpr $ toManyIdColumnVar toManyTable index foreignKey

handleToOnes :: UploadTable -> Array String
handleToOnes ut = do
  toOne <- ut.toOneTables
  handleToOne ut toOne


handleToOne :: UploadTable -> ToOne -> Array String
handleToOne ut (ToOne {foreignKey, table}) =
  handleToOnes table
  <> handleToManys table
  <> [ remark $ "find existing " <> table.tableName <> " records"
     , findExistingRecords wbTemplateMappingItemId table

     , remark $ "insert new " <> table.tableName <> " records"
     , insertNewRecords wbTemplateMappingItemId table

     , remark $ "find newly created " <> table.tableName <> " records"
     , findExistingRecords wbTemplateMappingItemId table
     ]
  where
    wbTemplateMappingItemId = varExpr $ toOneIdColumnVar ut.tableName foreignKey

insertNewRecords :: ScalarExpr -> UploadTable -> String
insertNewRecords wbTemplateMappingItemId table =
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


findExistingRecords :: ScalarExpr -> UploadTable -> String
findExistingRecords wbTemplateMappingItemId table =
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
foldl' f as = map (\{ head, tail } -> foldl1 f (head :| tail)) $ uncons as

insertIdFields :: UploadTable -> Array String
insertIdFields ut =
  [ remark "insert an id field for the base table"
  , insertValues [[wrap $ "now()", wrap $ "@templateid", stringLiteral ut.idColumn, stringLiteral ut.tableName]]
    ["timestampcreated", "workbenchtemplateid", "fieldname", "tablename"] "workbenchtemplatemappingitem"
  , setUserVar ut.idColumn (wrap "last_insert_id()")
  ] <>
  insertIdFieldsFromToOnes ut <>
  insertIdFieldsFromToManys ut

insertIdFieldsFromToOnes :: UploadTable -> Array String
insertIdFieldsFromToOnes ut = do
  (ToOne {foreignKey, table}) <- ut.toOneTables
  ([ remark $ "insert an id field for the " <> ut.tableName <> " to " <> table.tableName <> " foreign key"
   , insertValues
     [[wrap $ "now()", wrap $ "@templateid", stringLiteral foreignKey, stringLiteral ut.tableName]]
     ["timestampcreated", "workbenchtemplateid", "fieldname", "tablename"] "workbenchtemplatemappingitem"
   , setUserVar (toOneIdColumnVar ut.tableName foreignKey) (wrap "last_insert_id()")
   ] <>
   insertIdFieldsFromToOnes table <>
   insertIdFieldsFromToManys table
  )

toOneIdColumnVar :: String -> String -> String
toOneIdColumnVar tableName foreignKey = tableName <> "_" <> foreignKey

insertIdFieldsFromToManys :: UploadTable -> Array String
insertIdFieldsFromToManys ut = do
  {foreignKey, tableName, records} <- ut.toManyTables
  {index, record: {toOneTables}} <- mapWithIndex (\i r ->  {index: i, record: r}) records
  (ToOne {foreignKey, table}) <- toOneTables
  ([ remark $ "insert an id field for the " <> tableName <> show index <> " to " <> table.tableName <> " foreign key"
   , insertValues
     [[wrap $ "now()", wrap $ "@templateid", stringLiteral foreignKey, stringLiteral tableName]]
     ["timestampcreated", "workbenchtemplateid", "fieldname", "tablename"] "workbenchtemplatemappingitem"
   , setUserVar (toManyIdColumnVar tableName index foreignKey) (wrap "last_insert_id()")
   ] <>
   insertIdFieldsFromToOnes table <>
   insertIdFieldsFromToManys table
  )

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
