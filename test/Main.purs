module Test.Main where

import Data.SQL.Render
import Data.SQL.Smart
import Data.SQL.Syntax
import Prelude hiding (join)

import Debug.Trace (trace, traceM)
import Effect (Effect)
import Effect.Aff (launchAff_)
import Effect.Class.Console (log)
import ExamplePlan (uploadPlan)
import Test.Spec (describe, it)
import Test.Spec.Assertions (shouldEqual)
import Test.Spec.Reporter.Console (consoleReporter)
import Test.Spec.Runner (runSpec)
import Upload (MappingItem, findExistingRecords, rowsFromWB, rowsWithValuesFor, valuesFromWB)
import UploadPlan (ColumnType)

main :: Effect Unit
main = launchAff_ $ runSpec [consoleReporter] do
  describe "sql" do
    it "generates sql" do
      let query' = query [Star] `from` [table "foo" `as` (Alias "bar"), table "boo" `as` (Alias "booo") `join` (table "baz" `as` (Alias "baaaaz")) `on` (intLit 0)]
      render (renderQuery query') `shouldEqual` "foo"

    it "works for rowsWithValuesfor" do
      let query' = rowsWithValuesFor $ intLit 0
      render (renderQuery query') `shouldEqual` "foo"

    it "works for rowsFromWB" do
      let t = Alias "t"
      let excludedRows = rowsWithValuesFor $ intLit 0
      let query' = rowsFromWB (intLit 0) (map (parseMappingItem t) uploadPlan.uploadTable.mappingItems) excludedRows
      render (renderQuery query') `shouldEqual` "foo"

    it "works for valuesFromWB" do
      let t = Alias "t"
      let excludedRows = rowsWithValuesFor $ intLit 0
      let query' = valuesFromWB (intLit 0) (map (parseMappingItem t) uploadPlan.uploadTable.mappingItems) excludedRows
      render (renderQuery query') `shouldEqual` "foo"

    it "works for findExistingrecords" do
      let statement = findExistingRecords (intLit 0) uploadPlan.uploadTable
      log $ render (renderStatement statement)
      render (renderStatement statement) `shouldEqual` "foo"


parseMappingItem :: Alias -> {columnName :: String, columnType :: ColumnType, id :: Int} -> MappingItem
parseMappingItem t i =
  { mappingId: intLit i.id
  , tableAlias: t
  , columnType: i.columnType
  , selectFromWBas: i.columnName
  , tableColumn: i.columnName
  }
