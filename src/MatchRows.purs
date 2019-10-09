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

import Prelude hiding (join)

import Control.Monad.Writer (Writer, execWriter, tell)
import Data.Array (fromFoldable, intercalate, mapWithIndex, snoc, uncons)
import Data.Foldable (for_)
import Data.FoldableWithIndex (forWithIndex_)
import Data.Maybe (Maybe(..))
import Data.Newtype (wrap)
import Data.NonEmpty (foldl1, (:|))
import SQL (Alias(..), JoinExpr, Relation(..), ScalarExpr, SelectTerm(..), and, as, equal, insertFrom, insertValues, intLiteral, join, leftJoin, notInSubQuery, nullIf, plus, query, queryDistinct, setUserVar, strToDate, stringLiteral, tuple, varExpr, (..), (<=>))
import UploadPlan (ColumnType(..), TemplateId(..), ToManyRecord, ToOne(..), UploadPlan, UploadStrategy(..), UploadTable, WorkbenchId(..))

type MatchedRow = {recordid :: Int, rownumber :: Int, rowid :: Int}

type MappingItem = {tableColumn :: String, tableAlias :: Alias, selectFromWBas :: String, columnType :: ColumnType, mappingId :: ScalarExpr}

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


parseMappingItem :: Alias -> {columnName :: String, columnType :: ColumnType, id :: Int} -> MappingItem
parseMappingItem t i =
  { mappingId: intLiteral i.id
  , tableAlias: t
  , columnType: i.columnType
  , selectFromWBas: i.columnName
  , tableColumn: i.columnName
  }

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
    (snoc (joinToManys t table) joinWB)
    (foldl' and $ strategyToWhereClause table.strategy t # fromFoldable)
  )
  ["workbenchrowid", "celldata", "rownumber", "workbenchtemplatemappingitemid"]
  "workbenchdataitem"
  where
    t = Alias "t"
    wb = Alias "wb"
    mappingItems = map (parseMappingItem t) table.mappingItems <> toOneMappingItems table t <> toManyMappingItems table
    valsFromWB = tuple $ mappingItems <#> \{selectFromWBas} -> wb .. selectFromWBas
    valsFromTable = tuple $ mappingItems <#> \{tableColumn, tableAlias} -> tableAlias .. tableColumn
    excludeRows = rowsWithValuesFor wbTemplateMappingItemId
    joinWB = join (rowsFromWB (varExpr "workbenchid") mappingItems excludeRows) wb (Just $ valsFromWB <=> valsFromTable)

rowsFromWB :: ScalarExpr -> Array MappingItem -> Relation -> Relation
rowsFromWB wbId mappingItems excludeRows =
  query
  (mapWithIndex selectWBVal mappingItems <> [SelectTerm $ r .. "workbenchrowid", SelectTerm $ r .. "rownumber"])
  (Table "workbenchrow" `as` r)
  (mapWithIndex joinWBCell mappingItems)
  (Just $ ((r .. "workbenchid") `equal` wbId) `and` ((r .. "workbenchrowid") `notInSubQuery` excludeRows))
  where
    r = Alias "r"
    c i = Alias $ "c" <> (show i)
    selectWBVal i item = SelectAs item.selectFromWBas $ parseValue item.columnType $ c i .. "celldata"
    joinWBCell i item =
      leftJoin (Table "workbenchdataitem") (c i) $ Just $
      ((c i .. "workbenchrowid") `equal` (r .. "workbenchrowid")) `and`
      ((c i .. "workbenchtemplatemappingitemid") `equal` item.mappingId)

insertNewRecords :: ScalarExpr -> UploadTable -> Script
insertNewRecords wbTemplateMappingItemId table = tell $ pure $
  insertFrom
  ( query
    (newVals <> constantVals)
    (valuesFromWB (varExpr "workbenchid") (mappingItems <> toManyMappingItems table) excludeRows `as` nv)
    []
    Nothing
  )
  columns
  table.tableName
  where
    nv = Alias "newvalues"
    t = Alias "t" -- unused
    mappingItems = map (parseMappingItem t) table.mappingItems <> toOneMappingItems table t
    newVals = (\{tableColumn} -> SelectTerm $ nv .. tableColumn) `map` mappingItems
    constantVals = (\{columnName, value} -> SelectAs columnName $ wrap value) `map` table.staticValues
    columns = map _.tableColumn mappingItems <> map _.columnName table.staticValues
    excludeRows = rowsWithValuesFor wbTemplateMappingItemId

valuesFromWB :: ScalarExpr -> Array MappingItem -> Relation -> Relation
valuesFromWB wbId mappingItems excludeRows =
  queryDistinct
  (mapWithIndex selectWBVal mappingItems)
  (Table "workbenchrow" `as` r)
  (mapWithIndex joinWBCell mappingItems)
  (Just $ ((r .. "workbenchid") `equal` wbId) `and` ((r .. "workbenchrowid") `notInSubQuery` excludeRows))
  where
    r = Alias "r"
    c i = Alias $ "c" <> (show i)
    selectWBVal i item = SelectAs item.selectFromWBas $ parseValue item.columnType $ c i .. "celldata"
    joinWBCell i item =
      leftJoin (Table "workbenchdataitem") (c i) $ Just $
      ((c i .. "workbenchrowid") `equal` (r .. "workbenchrowid")) `and`
      ((c i .. "workbenchtemplatemappingitemid") `equal` item.mappingId)

joinToManys :: Alias -> UploadTable -> Array JoinExpr
joinToManys t ut = do
  {foreignKey, tableName, records} <- ut.toManyTables
  mapWithIndex (joinToMany t tableName foreignKey) records

joinToMany :: Alias -> String -> String -> Int -> ToManyRecord -> JoinExpr
joinToMany t tableName foreignKey index {filters} =
  leftJoin (Table tableName) alias $ foldl' and ([ (alias .. foreignKey) `equal` (t .. foreignKey) ] <> filterExprs)
  where
    alias = Alias $ tableName <> show index
    filterExprs = filters <#> \{columnName, value} -> (alias .. columnName) `equal` (wrap value)

toManyMappingItems :: UploadTable -> Array MappingItem
toManyMappingItems ut = do
  {foreignKey, tableName, records} <- ut.toManyTables
  mappingItems <- mapWithIndex (toManyRecordMappingItems tableName) records
  mappingItems

toManyRecordMappingItems :: String -> Int -> ToManyRecord -> Array MappingItem
toManyRecordMappingItems tableName index {mappingItems, toOneTables} =
  map (toManyToOneMappingItems tableName index) toOneTables
  <> map (parseToManyMappingItem tableName index) mappingItems

parseToManyMappingItem :: String -> Int -> {id :: Int, columnType :: ColumnType, columnName :: String} -> MappingItem
parseToManyMappingItem tableName index item =
  { selectFromWBas: tableName <> (show index) <> item.columnName
  , columnType: item.columnType
  , mappingId: intLiteral item.id
  , tableAlias: Alias $ tableName <> (show index)
  , tableColumn: item.columnName
  }

toManyToOneMappingItems :: String -> Int -> ToOne -> MappingItem
toManyToOneMappingItems tableName index (ToOne {foreignKey, table}) =
   { selectFromWBas: tableName <> (show index) <> foreignKey
   , columnType: IntType
   , mappingId: varExpr $ toManyIdColumnVar tableName index foreignKey
   , tableAlias: Alias $ tableName <> (show index)
   , tableColumn: foreignKey
   }

toOneMappingItems :: UploadTable -> Alias -> Array MappingItem
toOneMappingItems ut t = ut.toOneTables <#> \(ToOne {foreignKey, table}) ->
  {tableColumn: foreignKey
  , columnType: IntType
  , mappingId: varExpr $ toOneIdColumnVar ut.tableName foreignKey
  , tableAlias: t
  , selectFromWBas: foreignKey}

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
  for_ ut.toManyTables \{tableName, records} ->
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

parseValue :: ColumnType -> ScalarExpr -> ScalarExpr
parseValue StringType value = nullIf value (stringLiteral "")
parseValue DoubleType value = nullIf value (stringLiteral "") `plus` (wrap "0.0")
parseValue IntType value = nullIf value (stringLiteral "") `plus` (wrap "0")
parseValue DecimalType value = nullIf value (stringLiteral "")
parseValue (DateType format) value = strToDate value $ stringLiteral format
