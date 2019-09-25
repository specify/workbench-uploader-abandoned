module UploadPlan where

type MappingItem = {columnName :: String, columnType :: ColumnType, id :: Int}

data ColumnType = StringType | DoubleType | IntType | DecimalType


type UploadTable =
  { workbenchId :: Int
  , tableName :: String
  , idColumn :: String
  , filters :: Array { columnName :: String, value :: String }
  , mappingItems :: Array MappingItem
  , staticValues :: Array { columnName :: String, value :: String }
  }
