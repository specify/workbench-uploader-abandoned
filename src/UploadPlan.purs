module UploadPlan where

type MappingItem = {columnName :: String, columnType :: ColumnType, id :: Int}

data ColumnType = StringType | DoubleType | IntType | DecimalType | DateType

newtype ToOne = ToOne UploadTable

type UploadTable =
  { workbenchId :: Int
  , tableName :: String
  , idColumn :: String
  , filters :: Array { columnName :: String, value :: String }
  , mappingItems :: Array MappingItem
  , staticValues :: Array { columnName :: String, value :: String }
  , toOneTables :: Array { foreignKey :: String, table :: ToOne }
  }
