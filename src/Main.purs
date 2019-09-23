module Main where

import Prelude

import Data.Array (uncons, mapWithIndex)
import Data.Maybe (Maybe(..))
import Data.Newtype (wrap)
import Data.NonEmpty (foldl1, (:|))
import Effect (Effect)
import Effect.Console (logShow)
import SQL (JoinExpr, ScalarExpr, SelectExpr(..), SelectTerm(..), and, equal, isNull, join, leftJoin, nullIf, or, plus, query, (..))

main :: Effect Unit
main = do
  logShow matchRows


type MappingItem = {columnName :: String, columnType :: ColumnType, id :: Int}

data ColumnType = StringType | DoubleType | IntType | DecimalType


mappingItems :: Array MappingItem
mappingItems = [ {columnName: "shortName", columnType: StringType, id: 1907 }
               , {columnName: "localityName", columnType: StringType, id: 1908 }
               , {columnName: "Text2", columnType: StringType, id: 1913 }
               , {columnName: "verbatimElevation", columnType: StringType, id: 1930 }
               , {columnName: "minElevation", columnType: DoubleType, id: 1931 }
               , {columnName: "maxElevation", columnType: DoubleType, id: 1932 }
               , {columnName: "verbatimLatitude", columnType: StringType, id: 1938 }
               , {columnName: "latitude1", columnType: DecimalType, id: 1939 }
               , {columnName: "verbatimLongitude", columnType: StringType, id: 1940 }
               , {columnName: "longitude1", columnType: DecimalType, id: 1941 }
               ]

matchRows :: SelectExpr
matchRows = query [SelectTerm $ wrap "l.localityid", SelectTerm $ wrap "wb.rownumber"]
            (wrap "from locality l") joinWB (Just $ wrap "l.disciplineid = 3")
  where
    joinWB = case uncons $ map compValues mappingItems of
      Just { head: c, tail: cs } ->  [join rowsFromWB (wrap "wb") $ Just (foldl1 and (c :| cs))]
      Nothing -> []

rowsFromWB :: SelectExpr
rowsFromWB = query
             (mapWithIndex makeSelectWB mappingItems <> [SelectTerm $ wrap "r.rownumber"])
             (wrap "from workbenchrow r")
             (mapWithIndex makeJoinWB mappingItems)
             (Just $ wrap "r.workbenchid = 27")


compValues :: MappingItem -> ScalarExpr
compValues item = case item.columnType of
  StringType -> (v1 `equal` v2) `or` (isNull v1 `and` isNull v2) `or` (isNull v1 `and` (v2 `equal` wrap "''"))
  otherwise -> (v1 `equal` v2) `or` (isNull v1 `and` isNull v2)
  where v1 = wrap $ "l." <> item.columnName
        v2 = wrap $ "wb." <> item.columnName

makeJoinWB :: Int -> MappingItem -> JoinExpr
makeJoinWB i item = leftJoin (Table "workbenchdataitem") dataItem $ Just $
                    ((dataItem .. "workbenchrowid") `equal` (wrap "r.workbenchrowid")) `and`
                    ((dataItem .. "workbenchtemplatemappingitemid") `equal` (wrap $ show item.id))
  where dataItem = wrap ("c" <> (show i))

parseValue :: ColumnType -> ScalarExpr -> ScalarExpr
parseValue StringType value = value
parseValue DoubleType value =  nullIf value (wrap "''") `plus` (wrap "0.0")
parseValue IntType value = nullIf value (wrap "''") `plus` (wrap "0")
parseValue DecimalType value = nullIf value (wrap "''")

makeSelectWB :: Int -> MappingItem -> SelectTerm
makeSelectWB i item = SelectAs item.columnName (parseValue item.columnType value)
  where value = (wrap $ "c" <> (show i)) .. "celldata"


--   "select\n" <> (intercalate ",\n" $ mapWithIndex makeSelectWB mappingItems) <> """,
-- now()                                timestampcreated,
-- 0                                    srclatlongunit,
-- 3                                    disciplineid,
-- r.rownumber
-- from workbenchrow r
-- """ <> (intercalate "\n" $ mapWithIndex makeJoin mappingItems) <> """
-- where r.workbenchid = 27
-- """

