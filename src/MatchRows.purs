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
import SQL (Alias(..), JoinExpr, Relation(..), ScalarExpr(..), SelectTerm(..), and, as, equal, from, insertFrom, insertValues, intLiteral, isNull, join, leftJoin, notIn, notInSubQuery, nullIf, or, plus, query, queryDistinct, setUserVar, star, strToDate, stringLiteral, tuple, varExpr, (..), (<=>))
import UploadPlan (ColumnType(..), NamedValue, TemplateId(..), ToOne(..), UploadStrategy(..), UploadTable, WorkbenchId(..), UploadPlan)

type MatchedRow = {recordid :: Int, rownumber :: Int, rowid :: Int}

type MappingItem = {columnName :: String, columnType :: ColumnType, id :: ScalarExpr}

type Script = Writer (Array String) Unit

runPlan :: UploadPlan -> String
runPlan p =
  intercalate "\n\n" $ map (flip (<>) ";") $ execWriter $ script p

script :: UploadPlan -> Script
script up@{templateId: (TemplateId templateId), workbenchId: (WorkbenchId workbenchId)} = do
  tell [ "rollback" ]
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
     (mapWithIndex makeSelectWB mappingItems)
     (Table "workbenchrow" `as` r)
     (mapWithIndex makeJoinWB mappingItems)
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
    columns = map _.columnName mappingItems <> map _.columnName table.staticValues
    excludeMatched = (r .. "workbenchrowid") `notInSubQuery` (rowsWithValuesFor wbTemplateMappingItemId)
    mappingItems = map parseMappingItem table.mappingItems <> toOneMappingItems table

parseMappingItem :: {columnName :: String, columnType :: ColumnType, id :: Int} -> MappingItem
parseMappingItem i = i {id = intLiteral i.id}

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
    [join (rowsFromWB (varExpr "workbenchid") mappingItems) wb (Just $ valuesFromWB <=> valuesFromTable)]
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
    r = Alias "r"
    mappingItems = map parseMappingItem table.mappingItems <> toOneMappingItems table
    valuesFromWB = tuple $ mappingItems <#> \{columnName} -> wb .. columnName
    valuesFromTable = tuple $ mappingItems <#> \{columnName} -> t .. columnName

toOneMappingItems :: UploadTable -> Array MappingItem
toOneMappingItems ut = ut.toOneTables <#> \(ToOne {foreignKey, table}) ->
  {columnName: foreignKey, columnType: IntType, id: varExpr $ toOneIdColumnVar ut.tableName foreignKey}

strategyToWhereClause :: UploadStrategy -> Alias -> Maybe ScalarExpr
strategyToWhereClause strategy t = case strategy of
  AlwaysCreate -> Nothing
  AlwaysMatch values -> matchAll values
  MatchOrCreate values -> matchAll values
  where
    matchAll values = Just $ (tuple $ map (\v -> t .. v.columnName) values) <=> (tuple $ map (\v -> wrap v.value) values)


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

rowsFromWB :: ScalarExpr -> Array MappingItem -> Relation
rowsFromWB wbId mappingItems =
  query
  (mapWithIndex makeSelectWB mappingItems <> [SelectTerm $ r .. "workbenchrowid", SelectTerm $ r .. "rownumber"])
  (Table "workbenchrow" `as` r)
  (mapWithIndex makeJoinWB mappingItems)
  (Just $ (r .. "workbenchid") `equal` wbId)
  where (r :: Alias) = wrap "r"


makeJoinWB :: Int -> MappingItem -> JoinExpr
makeJoinWB i item = leftJoin (Table "workbenchdataitem") dataItem $ Just $
                    ((dataItem .. "workbenchrowid") `equal` (r .. "workbenchrowid")) `and`
                    ((dataItem .. "workbenchtemplatemappingitemid") `equal` item.id)
  where (dataItem :: Alias) = wrap ("c" <> (show i))
        (r :: Alias) = wrap "r"

parseValue :: ColumnType -> ScalarExpr -> ScalarExpr
parseValue StringType value = nullIf value (stringLiteral "")
parseValue DoubleType value = nullIf value (stringLiteral "") `plus` (wrap "0.0")
parseValue IntType value = nullIf value (stringLiteral "") `plus` (wrap "0")
parseValue DecimalType value = nullIf value (stringLiteral "")
parseValue (DateType format) value = strToDate value $ stringLiteral format

makeSelectWB :: Int -> MappingItem -> SelectTerm
makeSelectWB i item = SelectAs item.columnName (parseValue item.columnType value)
  where value = (wrap $ "c" <> (show i)) .. "celldata"
