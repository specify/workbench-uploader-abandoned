module MatchRows (matchRows, selectForInsert, insertNewVals) where

import Prelude

import Data.Array (intercalate, mapWithIndex, nub, uncons)
import Data.Maybe (Maybe(..))
import Data.Newtype (wrap)
import Data.NonEmpty (foldl1, (:|))
import SQL (Alias, JoinExpr, ScalarExpr(..), Relation(..), SelectTerm(..), and, as, equal, insertFrom, isNull, join, leftJoin, notIn, nullIf, or, plus, query, queryDistinct, star, (..))
import UploadPlan (ColumnType(..), MappingItem, UploadTable)



matchRows_ :: Int -> Array MappingItem -> Relation -> (Alias -> Maybe ScalarExpr) -> String -> Relation
matchRows_ wbId mappingItems matchTable whereExpr idCol =
  query [SelectTerm $ t .. idCol, SelectTerm $ wb .. "rownumber"] (matchTable `as` t) joinWB (whereExpr t)
  where
    (t :: Alias) = wrap "t"
    (wb :: Alias) = wrap "wb"
    joinWB = case uncons $ map compValues mappingItems of
      Just { head: c, tail: cs } ->  [join (rowsFromWB wbId mappingItems) wb $ Just (foldl1 and (c :| cs))]
      Nothing -> []

matchRows :: UploadTable -> Relation
matchRows ut = matchRows_ ut.workbenchId ut.mappingItems (Table ut.tableName) whereClause ut.idColumn
  where whereClause =
          \t -> map (\{head, tail} -> foldl1 and (head :| tail)) $
                uncons $ map (\{columnName, value} -> (t .. columnName) `equal` wrap value)
                ut.filters

rowsFromWB :: Int -> Array MappingItem -> Relation
rowsFromWB wbId mappingItems =
  query
  (mapWithIndex makeSelectWB mappingItems <> [SelectTerm $ r .. "rownumber"])
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
parseValue DoubleType value =  nullIf value (wrap "''") `plus` (wrap "0.0")
parseValue IntType value = nullIf value (wrap "''") `plus` (wrap "0")
parseValue DecimalType value = nullIf value (wrap "''")

makeSelectWB :: Int -> MappingItem -> SelectTerm
makeSelectWB i item = SelectAs item.columnName (parseValue item.columnType value)
  where value = (wrap $ "c" <> (show i)) .. "celldata"


selectNewVals :: Int -> Array MappingItem -> Array Int -> Relation
selectNewVals wbId mappingItems matched =
  queryDistinct
  (mapWithIndex makeSelectWB mappingItems)
  (Table "workbenchrow" `as` r)
  (mapWithIndex makeJoinWB mappingItems)
  (Just $ (r .. "workbenchid") `equal` (wrap $ show wbId) `and` (excludeMatched r matched))
  where (r :: Alias) = wrap "r"

selectForInsert :: Int -> Array MappingItem -> Array {columnName :: String, value :: String} -> Array Int -> Relation
selectForInsert wbId mappingItems extraFields matched =
  query ([star] <> extraFields_)
  (selectNewVals wbId mappingItems matched `as` newValues)
  []
  Nothing
  where (newValues :: Alias) = wrap "newvalues"
        extraFields_ = (\{columnName, value} -> SelectAs columnName $ wrap value) `map` extraFields

excludeMatched :: Alias -> Array Int -> ScalarExpr
excludeMatched row matched = (row .. "rownumber") `notIn` rowList
  where rowList = map (show >>> wrap) $ nub matched

insertNewVals_ :: String -> Int -> Array MappingItem -> Array {columnName :: String, value :: String} -> Array Int -> String
insertNewVals_ table wbId mappingItems extraFields matched =
  insertFrom (selectForInsert wbId mappingItems extraFields matched) columns table
  where columns = map _.columnName mappingItems <> map _.columnName extraFields



insertNewVals :: UploadTable -> Array Int -> String
insertNewVals ut matchedRows =
  insertNewVals_ ut.tableName ut.workbenchId ut.mappingItems ut.staticValues matchedRows
