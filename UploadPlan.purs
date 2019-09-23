module UploadPlan where

type UploadPlan = { rootTable :: String, directives :: Array Directive }

type DataSetField = { column :: Int, caption :: String, fieldName :: String }

data Directive = CreateToOne { fieldName :: String, directives :: Array Directive }
               | MatchToOne { fieldName :: String, directives :: Array MatchToOneDirective }
               | CreateOrMatchToOne { fieldName :: String, directives :: Array CreateOrMatchToOneDirective }
               | CreateToMany { fieldName :: String, records :: Array Record }
               | FixedField { fieldName :: String, value :: String }
               | DataSetField DataSetField

data MatchToOneDirective = MatchField DataSetField
                         | IgnoreField DataSetField
                         | MatchUnlessEmptyField DataSetField


data CreateOrMatchToOneDirective = MatchExistingField DataSetField
                                 | UseIfCreating DataSetField
                                 | DontMatchIfEmpty DataSetField
                                 | MatchToOne' { fieldName :: String, directives :: Array MatchToOneDirective }
                                 | CreateOrMatchToOne' { fieldName :: String, directives :: Array CreateOrMatchToOneDirective }


data Record = Record (Array Directive)


examplePlan :: UploadPlan
examplePlan =
  { rootTable: "CollectionObject"
  , directives:
    [ DataSetField { column: 1, caption: "catalog number", fieldName: "catalognumber"}
    , CreateToOne { fieldName: "collectingevent"
                  , directives:
                    [ DataSetField { column: 2, caption: "collector number", fieldName: "stationfieldnumber" }
                    , DataSetField { column: 3, caption: "date", fieldName: "startdate" }
                    , MatchToOne { fieldName: "locality"
                                 , directives:
                                   [ MatchField { column: 4, caption: "location", fieldName: "localityname" }
                                   , MatchField { column: 5, caption: "lat", fieldName: "latitude1" }
                                   , MatchField { column: 6, caption: "long", fieldName: "longitude1" }
                                   , IgnoreField { column: 20, caption: "locality remarks", fieldName: "remarks" }
                                   , MatchUnlessEmptyField { column: 21, caption: "habitat", fieldName: "namedplace"}
                                   ]
                                 }
                    , CreateToMany { fieldName: "collectors"
                                   , records:
                                     [ Record
                                       [ FixedField { fieldName: "ordernumber", value: "0" }
                                       , FixedField { fieldName: "isprimary", value: "true" }
                                       , CreateOrMatchToOne { fieldName: "agent"
                                                            , directives:
                                                              [ MatchExistingField { column: 11, caption: "collector 1", fieldName: "lastname"} ]
                                                            }
                                       ]
                                     , Record
                                       [ FixedField { fieldName: "ordernumber", value: "1" }
                                       , FixedField { fieldName: "isprimary", value: "false" }
                                       , CreateOrMatchToOne { fieldName: "agent"
                                                            , directives:
                                                              [ MatchExistingField { column: 12, caption: "collector 2", fieldName: "lastname"} ]
                                                            }
                                       ]
                                     ]
                                   }
                    ]
                  }
    , CreateToMany { fieldName: "determinations"
                   , records:
                     [ Record
                       [ DataSetField { column: 7, caption: "determination date 1", fieldName: "determineddate" }
                       , MatchToOne { fieldName: "determiner"
                                    , directives:
                                      [ MatchField { column: 8, caption: "determiner lastname 1", fieldName: "lastname"} ]
                                    }
                       ]
                     , Record
                       [ DataSetField { column: 9, caption: "determination date 2", fieldName: "determineddate" }
                       , MatchToOne { fieldName: "determiner"
                                    , directives:
                                      [ MatchField { column: 10, caption: "determiner lastname 2", fieldName: "lastname"} ]
                                    }
                       ]
                     ]
                   }
    ]
  }

uconnPlan :: UploadPlan
uconnPlan =
  { rootTable: "CollectionObject"
  , directives:
    [ DataSetField { column:  0, caption: "accession_no_verbatim", fieldName: "altCatalogNumber" }
    , DataSetField { column: 18, caption: "creation_date", fieldName: "catalogedDate" }
    , DataSetField { column: 19, caption: "curatorial_notes", fieldName: "collectionObjectRemarks" }
    , DataSetField { column: 20, caption: "data_entered_by", fieldName: "catalogerLastName" }
    , DataSetField { column: 21, caption: "data_modified_by", fieldName: "inventorizedByLastName" }
    , DataSetField { column: 47, caption: "miscellaneous", fieldName: "description" }
    , DataSetField { column: 48, caption: "name_on_label", fieldName: "name" }
    , DataSetField { column: 50, caption: "original_collection", fieldName: "collectionObjectText2" }
    , DataSetField { column: 51, caption: "original_collection_no", fieldName: "collectionObjectText3" }
    , DataSetField { column: 65, caption: "specimen_condition", fieldName: "objectCondition" }

    , MatchToOne { fieldName: "accession"
                 , directives: [ MatchField { column:  1, caption: "accession_no", fieldName: "number" } ]
                 }

    , CreateToOne { fieldName: "collectionobjectattribute"
                  , directives:
                    [ DataSetField { column:  2, caption: "age_verbatim", fieldName: "colObjAttributeText13" }
                    , DataSetField { column:  3, caption: "age", fieldName: "colObjAttributeText2" }
                    , DataSetField { column: 26, caption: "ear_length_verbatim", fieldName: "colObjAttributeText14" }
                    , DataSetField { column: 27, caption: "ear_length", fieldName: "colObjAttributeText7" }
                    , DataSetField { column: 33, caption: "gonad_length_verbatim", fieldName: "colObjAttributeText15" }
                    , DataSetField { column: 34, caption: "gonad_length", fieldName: "colObjAttributeText3" }
                    , DataSetField { column: 35, caption: "gonad_width_verbatim", fieldName: "colObjAttributeText16" }
                    , DataSetField { column: 36, caption: "gonad_width", fieldName: "colObjAttributeText4" }
                    , DataSetField { column: 41, caption: "hind_foot_length_verbatim", fieldName: "colObjAttributeText17" }
                    , DataSetField { column: 44, caption: "mass_verbatim", fieldName: "colObjAttributeText18" }
                    , DataSetField { column: 45, caption: "mass1", fieldName: "colObjAttributeText10" }
                    , DataSetField { column: 46, caption: "mass2", fieldName: "colObjAttributeText19" }
                    , DataSetField { column: 49, caption: "hind_foot_length", fieldName: "colObjAttributeText5" }
                    , DataSetField { column: 52, caption: "other_measurements", fieldName: "colObjAttributeRemarks" }
                    , DataSetField { column: 63, caption: "sex_verbatim", fieldName: "colObjAttributeText20" }
                    , DataSetField { column: 68, caption: "tail_length_verbatim", fieldName: "colObjAttributeText21" }
                    , DataSetField { column: 69, caption: "tail_length", fieldName: "colObjAttributeText6" }
                    , DataSetField { column: 70, caption: "total_length_verbatim", fieldName: "colObjAttributeText22" }
                    , DataSetField { column: 71, caption: "total_length", fieldName: "colObjAttributeText8" }
                    , DataSetField { column: 72, caption: "reproductive_status", fieldName: "colObjAttributeText9" }
                    , DataSetField { column: 75, caption: "sex", fieldName: "colObjAttributeText1" }
                    ]
                  }
    , CreateOrMatchToOne { fieldName: "collectingevent"
                         , directives:
                           [ MatchExistingField { column:  8, caption: "collection_date_verbatim", fieldName: "verbatimDate" }
                           , MatchExistingField { column:  9, caption: "collection_date", fieldName: "startDate" }
                           , MatchExistingField { column: 10, caption: "collection_date2_verbatim", fieldName: "endDateVerbatim" }
                           , MatchExistingField { column: 11, caption: "collection_date2", fieldName: "endDate" }
                           , MatchExistingField { column: 13, caption: "collection_method", fieldName: "method" }
                           , MatchExistingField { column: 14, caption: "collector", fieldName: "collectorLastName1" }
                           , MatchExistingField { column: 15, caption: "collector_number", fieldName: "stationFieldNumber" }
                           , MatchExistingField { column: 22, caption: "detailed_locality", fieldName: "verbatimLocality" }
                           , MatchExistingField { column: 28, caption: "ecological_notes", fieldName: "collectingEventRemarks" }
                           , MatchExistingField { column: 73, caption: "expedition_number", fieldName: "text2" }
                           ]
                         }
    ]
  }
