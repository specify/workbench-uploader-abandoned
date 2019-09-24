module Main where

import Prelude

import Data.Array (uncons, mapWithIndex)
import Data.Maybe (Maybe(..))
import Data.Newtype (wrap)
import Data.NonEmpty (foldl1, (:|))
import Data.Traversable (for_)
import Effect (Effect)
import Effect.Aff (launchAff_)
import Effect.Class (liftEffect)
import Effect.Console (logShow)
import MySQL.Connection (closeConnection, createConnection, defaultConnectionInfo, query_)
import SQL (Alias, JoinExpr, ScalarExpr, SelectExpr(..), SelectTerm(..), and, as, equal, isNull, join, leftJoin, nullIf, or, plus, query, (..))

main :: Effect Unit
main = do
  logShow matchRows
  conn <- createConnection $ defaultConnectionInfo {database = "uconnverts", password = "Master", user = "Master"}
  launchAff_ do
    result :: Array {localityid :: Int, rownumber :: Int} <- query_ (show matchRows) conn
    for_ result \r -> do
      liftEffect $ logShow r
    liftEffect $ closeConnection conn


type MappingItem = {columnName :: String, columnType :: ColumnType, id :: Int}

data ColumnType = StringType | DoubleType | IntType | DecimalType


mappingItems' :: Array MappingItem
mappingItems' = [ {columnName: "shortName", columnType: StringType, id: 1907 }
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
matchRows = matchRows' mappingItems' (Table "locality") (\t -> Just $ (t .. "disciplineid") `equal` wrap "3") "localityid"

matchRows' :: Array MappingItem -> SelectExpr -> (Alias -> Maybe ScalarExpr) -> String -> SelectExpr
matchRows' mappingItems matchTable whereExpr idCol =
  query [SelectTerm $ t .. idCol, SelectTerm $ wb .. "rownumber"] (matchTable `as` t) joinWB (whereExpr t)
  where
    (t :: Alias) = wrap "t"
    (wb :: Alias) = wrap "wb"
    joinWB = case uncons $ map compValues mappingItems of
      Just { head: c, tail: cs } ->  [join (rowsFromWB mappingItems) wb $ Just (foldl1 and (c :| cs))]
      Nothing -> []

rowsFromWB :: Array MappingItem -> SelectExpr
rowsFromWB mappingItems = query
                          (mapWithIndex makeSelectWB mappingItems <> [SelectTerm $ r .. "rownumber"])
                          (Table "workbenchrow" `as` r)
                          (mapWithIndex makeJoinWB mappingItems)
                          (Just $ (r .. "workbenchid") `equal` wrap "27")
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


--   "select\n" <> (intercalate ",\n" $ mapWithIndex makeSelectWB mappingItems) <> """,
-- now()                                timestampcreated,
-- 0                                    srclatlongunit,
-- 3                                    disciplineid,
-- r.rownumber
-- from workbenchrow r
-- """ <> (intercalate "\n" $ mapWithIndex makeJoin mappingItems) <> """
-- where r.workbenchid = 27
-- """

