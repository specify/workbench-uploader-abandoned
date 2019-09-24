module UploadPlan where

type MappingItem = {columnName :: String, columnType :: ColumnType, id :: Int}

data ColumnType = StringType | DoubleType | IntType | DecimalType

