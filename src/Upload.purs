module Upload where

import Data.SQL.Smart
import Data.SQL.Syntax
import Prelude hiding (join)

import Data.Array (foldl, mapWithIndex, uncons)
import Data.FoldableWithIndex (foldlWithIndex)
import Data.Maybe (Maybe(..))
import Data.NonEmpty (foldl1, (:|))
import UploadPlan (ColumnType(..), TemplateId(..), ToManyRecord, ToOne(..), UploadPlan, UploadStrategy(..), UploadTable, WorkbenchId(..), ToMany)

foldl' :: forall a. (a -> a -> a) -> Array a -> Maybe a
foldl' f as = (\{ head, tail } -> foldl1 f (head :| tail)) <$> uncons as


type MatchedRow = {recordid :: Int, rownumber :: Int, rowid :: Int}

type MappingItem = {tableColumn :: String, tableAlias :: Alias, selectFromWBas :: String, columnType :: ColumnType, mappingId :: Expr}

parseMappingItem :: Alias -> {columnName :: String, columnType :: ColumnType, id :: Int} -> MappingItem
parseMappingItem t i =
  { mappingId: intLit i.id
  , tableAlias: t
  , columnType: i.columnType
  , selectFromWBas: i.columnName
  , tableColumn: i.columnName
  }

rowsWithValuesFor :: Expr -> QueryExpr
rowsWithValuesFor workbenchtemplatemappingitemid =
  query [select $ r .. "workbenchrowid"]
  `from`
  [ table "workbenchrow" `as` r
    `join` (table "workbenchdataitem" `as` d)
    `on`
    (((d .. "workbenchrowid") `equal` (r .. "workbenchrowid"))
     `and`
     ((d .. "workbenchtemplatemappingitemid") `equal` workbenchtemplatemappingitemid)
    )
  ]
  where
    r = Alias "r"
    d = Alias "d"


maybeApply :: forall a b. (a -> b -> a) -> a -> Maybe b -> a
maybeApply f a mb =
  case mb of
    Just b -> f a b
    Nothing -> a

findExistingRecords :: Expr -> UploadTable -> Statement
findExistingRecords wbTemplateMappingItemId ut =
  insertFrom "workbenchdataitem" ["workbenchrowid", "celldata", "rownumber", "workbenchtemplatemappingitemid"] $
  query
  [ selectAs "rowid" $ wb .. "workbenchrowid"
  , selectAs "id" $ t .. ut.idColumn
  , select $ wb .. "rownumber"
  , select $ wbTemplateMappingItemId
  ] `from`
  [ ((table ut.tableName `as` t) # joinToManys t ut) `join` wbSubQuery `on` (valuesFromWB <=> valuesFromTable)]
  `maybeApply suchThat` strategyToWhereClause ut.strategy t
  where
    t = Alias "t"
    wb = Alias "wb"
    mappingItems = map (parseMappingItem t) ut.mappingItems <> toOneMappingItems ut t <> toManyMappingItems ut
    valuesFromWB = row $ mappingItems <#> \item -> wb .. item.selectFromWBas
    valuesFromTable = row $ mappingItems <#> \item -> item.tableAlias .. item.tableColumn
    wbSubQuery = subqueryAs wb $ rowsFromWB (userVar "workbenchid") mappingItems excludeRows
    excludeRows = rowsWithValuesFor wbTemplateMappingItemId

strategyToWhereClause :: UploadStrategy -> Alias -> Maybe Expr
strategyToWhereClause strategy t = case strategy of
  AlwaysCreate -> Nothing
  AlwaysMatch values -> matchAll values
  MatchOrCreate values -> matchAll values
  where
    matchAll values = Just $ (row $ map (\v -> t .. v.columnName) values) <=> (row $ map (\v -> stringLit v.value) values)

toManyMappingItems :: UploadTable -> Array MappingItem
toManyMappingItems ut = do
  {foreignKey, tableName, records} <- ut.toManyTables
  mappingItems <- mapWithIndex (toManyRecordMappingItems tableName) records
  mappingItems

toManyRecordMappingItems :: String -> Int -> ToManyRecord -> Array MappingItem
toManyRecordMappingItems tableName index {mappingItems, toOneTables} =
  map (toManyToOneMappingItems tableName index) toOneTables
  <> map (parseToManyMappingItem tableName index) mappingItems

toManyToOneMappingItems :: String -> Int -> ToOne -> MappingItem
toManyToOneMappingItems tableName index (ToOne {foreignKey, table}) =
   { selectFromWBas: tableName <> (show index) <> foreignKey
   , columnType: IntType
   , mappingId: userVar $ toManyIdColumnVar tableName index foreignKey
   , tableAlias: Alias $ tableName <> (show index)
   , tableColumn: foreignKey
   }

parseToManyMappingItem :: String -> Int -> {id :: Int, columnType :: ColumnType, columnName :: String} -> MappingItem
parseToManyMappingItem tableName index item =
  { selectFromWBas: tableName <> (show index) <> item.columnName
  , columnType: item.columnType
  , mappingId: intLit item.id
  , tableAlias: Alias $ tableName <> (show index)
  , tableColumn: item.columnName
  }

toOneMappingItems :: UploadTable -> Alias -> Array MappingItem
toOneMappingItems ut t = ut.toOneTables <#> \(ToOne {foreignKey, table}) ->
  {tableColumn: foreignKey
  , columnType: IntType
  , mappingId: userVar $ toOneIdColumnVar ut.tableName foreignKey
  , tableAlias: t
  , selectFromWBas: foreignKey}

toOneIdColumnVar :: String -> String -> String
toOneIdColumnVar tableName foreignKey = tableName <> "_" <> foreignKey

toManyIdColumnVar :: String -> Int -> String -> String
toManyIdColumnVar tableName index foreignKey = tableName <> show index <> "_" <> foreignKey

joinToManys :: Alias -> UploadTable -> TableRef -> TableRef
joinToManys t ut tr =
  foldl (joinToManyTable t) tr ut.toManyTables

joinToManyTable :: Alias -> TableRef -> ToMany -> TableRef
joinToManyTable t tr {foreignKey, tableName, records} =
  foldlWithIndex (joinToMany t tableName foreignKey) tr records

joinToMany :: Alias -> String -> String -> Int -> TableRef -> ToManyRecord -> TableRef
joinToMany t tableName foreignKey index tr {filters} =
  tr `leftJoin` (table tableName `as` alias) `on` foldl and usingForeignKey filterExprs
  where
    alias = Alias $ tableName <> show index
    usingForeignKey = (alias .. foreignKey) `equal` (t .. foreignKey)
    filterExprs = filters <#> \{columnName, value} -> (alias .. columnName) `equal` stringLit value

rowsFromWB :: Expr -> Array MappingItem -> QueryExpr -> QueryExpr
rowsFromWB wbId mappingItems excludeRows =
  query (mapWithIndex selectWBVal mappingItems <> [select $ r .. "workbenchrowid", select $ r .. "rownumber"])
  `from`
  [ (table "workbenchrow" `as` r) `foldlWithIndex joinWBCell` mappingItems ]
  `suchThat`
  (((r .. "workbenchid") `equal` wbId) `and` ((r .. "workbenchrowid") `notInSubQuery` excludeRows))
  where
    r = Alias "r"
    c i = Alias $ "c" <> (show i)
    selectWBVal i item = selectAs item.selectFromWBas $ parseValue item.columnType $ c i .. "celldata"
    joinWBCell :: Int -> TableRef -> MappingItem -> TableRef
    joinWBCell i left item =
      left `leftJoin` (table "workbenchdataitem" `as` c i)
      `on`
      ( ((c i .. "workbenchrowid") `equal` (r .. "workbenchrowid"))
        `and` ((c i .. "workbenchtemplatemappingitemid") `equal` item.mappingId)
      )

valuesFromWB :: Expr -> Array MappingItem -> QueryExpr -> QueryExpr
valuesFromWB wbId mappingItems excludeRows =
  queryDistinct (mapWithIndex selectWBVal mappingItems)
  `from`
  [ (table "workbenchrow" `as` r) `foldlWithIndex joinWBCell` mappingItems ]
  `suchThat`
  (((r .. "workbenchid") `equal` wbId) `and` ((r .. "workbenchrowid") `notInSubQuery` excludeRows))
  `orderBy`
  (mappingItems <#> \i -> asc $ project $ i.selectFromWBas)
  where
    r = Alias "r"
    c i = Alias $ "c" <> (show i)
    selectWBVal i item = selectAs item.selectFromWBas $ parseValue item.columnType $ c i .. "celldata"
    joinWBCell :: Int -> TableRef -> MappingItem -> TableRef
    joinWBCell i left item =
      left `leftJoin` (table "workbenchdataitem" `as` c i)
      `on`
      ( ((c i .. "workbenchrowid") `equal` (r .. "workbenchrowid"))
        `and` ((c i .. "workbenchtemplatemappingitemid") `equal` item.mappingId)
      )



parseValue :: ColumnType -> Expr -> Expr
parseValue StringType value = nullIf value (stringLit "")
parseValue DoubleType value = nullIf value (stringLit "") `plus` (floatLit 0.0)
parseValue IntType value = nullIf value (stringLit "") `plus` (intLit 0)
parseValue DecimalType value = nullIf value (stringLit "")
parseValue (DateType format) value = strToDate value $ stringLit format
