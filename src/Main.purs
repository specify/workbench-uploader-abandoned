module Main where

import Prelude

import Data.Maybe (Maybe(..))
import Data.Newtype (wrap)
import Data.Traversable (for_)
import Effect (Effect)
import Effect.Aff (launchAff_)
import Effect.Class (liftEffect)
import Effect.Console (logShow)
import UploadPlan (ColumnType(..), MappingItem)
import MatchRows (matchRows)
import MySQL.Connection (closeConnection, createConnection, defaultConnectionInfo, query_)
import SQL (SelectExpr(..), equal, (..))

main :: Effect Unit
main = do
  logShow matchRows_
  conn <- createConnection $ defaultConnectionInfo {database = "uconnverts", password = "Master", user = "Master"}
  launchAff_ do
    result :: Array {localityid :: Int, rownumber :: Int} <- query_ (show matchRows_) conn
    for_ result \r -> do
      liftEffect $ logShow r
    liftEffect $ closeConnection conn


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

matchRows_ :: SelectExpr
matchRows_ = matchRows 27 mappingItems (Table "locality") (\t -> Just $ (t .. "disciplineid") `equal` wrap "3") "localityid"

--   "select\n" <> (intercalate ",\n" $ mapWithIndex makeSelectWB mappingItems) <> """,
-- now()                                timestampcreated,
-- 0                                    srclatlongunit,
-- 3                                    disciplineid,
-- r.rownumber
-- from workbenchrow r
-- """ <> (intercalate "\n" $ mapWithIndex makeJoin mappingItems) <> """
-- where r.workbenchid = 27
-- """

