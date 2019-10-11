module Upload where

import Prelude hiding (join)
import SqlLang
import SqlSmart

import Data.Array (foldl, mapWithIndex)
import Data.FoldableWithIndex (foldlWithIndex)
import UploadPlan (ColumnType(..), TemplateId(..), ToManyRecord, ToOne(..), UploadPlan, UploadStrategy(..), UploadTable, WorkbenchId(..))

type MatchedRow = {recordid :: Int, rownumber :: Int, rowid :: Int}

type MappingItem = {tableColumn :: String, tableAlias :: Alias, selectFromWBas :: String, columnType :: ColumnType, mappingId :: Expr}


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
