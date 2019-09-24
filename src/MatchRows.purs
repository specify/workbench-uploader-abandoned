module MatchRows (matchRows) where

import Prelude

import Data.Array (uncons, mapWithIndex)
import Data.Maybe (Maybe(..))
import Data.Newtype (wrap)
import Data.NonEmpty (foldl1, (:|))
import SQL (Alias, JoinExpr, ScalarExpr, SelectExpr(..), SelectTerm(..), and, as, equal, isNull, join, leftJoin, nullIf, or, plus, query, (..))
import UploadPlan (ColumnType(..), MappingItem)



matchRows :: Int -> Array MappingItem -> SelectExpr -> (Alias -> Maybe ScalarExpr) -> String -> SelectExpr
matchRows wbId mappingItems matchTable whereExpr idCol =
  query [SelectTerm $ t .. idCol, SelectTerm $ wb .. "rownumber"] (matchTable `as` t) joinWB (whereExpr t)
  where
    (t :: Alias) = wrap "t"
    (wb :: Alias) = wrap "wb"
    joinWB = case uncons $ map compValues mappingItems of
      Just { head: c, tail: cs } ->  [join (rowsFromWB wbId mappingItems) wb $ Just (foldl1 and (c :| cs))]
      Nothing -> []

rowsFromWB :: Int -> Array MappingItem -> SelectExpr
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


