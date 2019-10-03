module ExamplePlan where

import Prelude

import UploadPlan (ColumnType(..), TemplateId(..), ToManyRecord, ToOne(..), UploadPlan, UploadStrategy(..), WorkbenchId(..))

uploadPlan :: UploadPlan
uploadPlan =
  { workbenchId: WorkbenchId 25
  , templateId: TemplateId 25
  , uploadTable:
    { tableName: "collectingevent"
    , idColumn: "collectingeventid"
    , strategy: AlwaysCreate
    , mappingItems:
      [ {columnName: "remarks", id: 1349, columnType: StringType}
      -- , {columnName: "endDate", id: 1352, columnType: DateType "%d %b %Y"}
      , {columnName: "method", id: 1343, columnType: StringType}
      -- , {columnName: "startDate", id: 1346, columnType: DateType "%d %b %Y"}
      , {columnName: "verbatimDate", id: 1366, columnType: StringType}
      ]
    , staticValues:
      [ { columnName: "disciplineid", value: "32768" }
      , { columnName: "timestampcreated", value: "now()" }
      , { columnName: "guid", value: "uuid()" }
      ]
    , toOneTables:
      [ ToOne
        { foreignKey: "localityid"
        , table: { tableName: "locality"
                 , idColumn: "localityid"
                 , strategy: MatchOrCreate [{columnName: "disciplineid", value: "32768"}]
                 , mappingItems:
                   [ {columnName: "text1", id: 1359, columnType: StringType}
                   , {columnName: "lat1text", id: 1340, columnType: StringType}
                   , {columnName: "localityName", id: 1358, columnType: StringType}
                   , {columnName: "long1text", id: 1338, columnType: StringType}
                   , {columnName: "maxElevation", id: 1357, columnType: DoubleType}
                   , {columnName: "minElevation", id: 1361, columnType: DoubleType}
                   , {columnName: "originalElevationUnit", id: 1348, columnType: StringType}
                   ]
                 , staticValues:
                   [ {columnName: "srclatlongunit", value: "0"}
                   , {columnName: "disciplineid", value: "32768"}
                   , {columnName: "timestampcreated", value: "now()"}
                   , { columnName: "guid", value: "uuid()" }
                   ]
                 , toOneTables: []
                 , toManyTables: []
                 }
        }
      ]
    , toManyTables:
      [ { foreignKey: "collectingeventid"
        , tableName: "collector"
        , records: [collectorRecord 0 1351, collectorRecord 1 1339, collectorRecord 2 1353]
        }
      ]
    }
  }

collectorRecord :: Int -> Int -> ToManyRecord
collectorRecord ordernumber id =
  { filters:
    [ {columnName: "divisionid", value: "2"}
    , {columnName: "ordernumber", value: show ordernumber}
    ]
  , staticValues:
    [ {columnName: "divisionid", value: "2"}
    , {columnName: "ordernumber", value: show ordernumber}
    ]
  , mappingItems: []
  , toOneTables:
    [ ToOne
      { foreignKey: "agentid"
      , table: { tableName: "agent"
               , idColumn: "agentid"
               , strategy: MatchOrCreate [{columnName: "divisionid", value: "2"}]
               , mappingItems: [ {columnName: "lastname", id: id, columnType: StringType} ]
               , staticValues:
                 [ {columnName: "divisionid", value: "2"}
                 , {columnName: "timestampcreated", value: "now()"}
                 , {columnName: "guid", value: "uuid()"}
                 , {columnName: "agentType", value: "2"}
                 ]
               , toOneTables: []
               , toManyTables: []
               }
      }
    ]
  }
