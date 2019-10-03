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

type NamedValue = { columnName :: String, value :: String }

type UploadTable =
  { tableName :: String
  , idColumn :: String
  , strategy :: UploadStrategy
  , mappingItems :: Array MappingItem
  , staticValues :: Array NamedValue
  , toOneTables :: Array ToOne
  , toManyTables :: Array ToMany
  }

data UploadStrategy = AlwaysCreate
                    | AlwaysMatch (Array NamedValue)
                    | MatchOrCreate (Array NamedValue)

type ToMany = { foreignKey :: String, tableName :: String, records :: Array ToManyRecord }

newtype ToOne = ToOne { foreignKey :: String, table :: UploadTable }

type ToManyRecord =
  { filters :: Array NamedValue
  , staticValues :: Array NamedValue
  , toOneTables :: Array ToOne
  , mappingItems :: Array MappingItem
  }
