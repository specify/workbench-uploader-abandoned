module UploadPlan where

type MappingItem = {columnName :: String, columnType :: ColumnType, id :: Int}

data ColumnType = StringType
                | DoubleType
                | IntType
                | DecimalType
                | DateType String

newtype WorkbenchId = WorkbenchId Int
newtype TemplateId = TemplateId Int

type UploadPlan =
  { workbenchId :: WorkbenchId
  , templateId :: TemplateId
  , uploadTable :: UploadTable
  }

type UploadTable =
  { tableName :: String
  , idColumn :: String
  , filters :: Array { columnName :: String, value :: String }
  , mappingItems :: Array MappingItem
  , staticValues :: Array { columnName :: String, value :: String }
  , toOneTables :: Array ToOne
  , toManyTables :: Array ToMany
  }

type ToMany = { foreignKey :: String, tableName :: String, records :: Array ToManyRecord }

newtype ToOne = ToOne { foreignKey :: String, table :: UploadTable }

type ToManyRecord =
  { filters :: Array { columnName :: String, value :: String }
  , staticValues :: Array { columnName :: String, value :: String }
  , toOneTables :: Array ToOne
  , mappingItems :: Array MappingItem
  }
