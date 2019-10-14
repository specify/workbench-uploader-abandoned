module Data.SQL.Syntax where

import Data.Maybe (Maybe)
import Data.NonEmpty (NonEmpty)


data Statement
  = QueryStatement QueryExpr
  | InsertValues InsertValues
  | InsertFrom InsertFrom

type InsertValues =
  { table :: TableName
  , columns :: Array ColumnName
  , values :: Array (Array Expr)
  }

type InsertFrom =
  { table :: TableName
  , columns :: Array ColumnName
  , query :: QueryExpr
  }

data SelectType
  = SelectAll
  | SelectDistinct
  | SelectDistinctRow

newtype QueryExpr = QueryExpr
  { selectType :: SelectType
  , selectTerms :: Array SelectTerm
  , from :: NonEmpty Array TableRef
  , where_ :: Maybe Expr
  , orderBy :: Array OrderTerm
  , limit :: Maybe LimitExpr
  }

data SelectTerm
  = SelectTerm Expr
  | SelectAs String Expr
  | Star

data OrderTerm = Ascending Expr | Descending Expr

newtype LimitExpr = LimitExpr {}

data TableRef = TableFactor TableFactor | JoinedTable JoinedTable

data TableFactor
  = Table {name :: TableName, as :: Maybe Alias}
  | SubQuery SubQuery
  | Tables (Array TableRef)
  | Dual

type SubQuery = {query :: QueryExpr, as :: Alias, columns :: Array ColumnName}

newtype TableName = TableName String
newtype Alias = Alias String
newtype ColumnName = ColumnName String

data JoinedTable
  = InnerJoin TableRef TableRef (Maybe JoinSpec)
  | LeftJoin TableRef TableRef JoinSpec
  | RightJoin TableRef TableRef JoinSpec
  | NaturalLeftJoin TableRef TableRef
  | NaturalRightJoin TableRef TableRef


data JoinSpec = JoinOn Expr | JoinUsing (Array ColumnName)

data BinOp = AndOp | OrOp | PlusOp | MinusOp

data UnaryOp = NotOp | IsNullOp

data CompOp = E_Op | GE_Op | G_Op | LE_Op | L_Op | NE_Op


data Expr
  = OrExpr Expr Expr
  | XorExpr Expr Expr
  | AndExpr Expr Expr
  | NotExpr Expr
  | IsTrueExpr Expr
  | IsFalseExpr Expr
  | IsUnknownExpr Expr
  | IsNull Expr
  | IsNotNull Expr
  | IsNotDistinctFrom Expr Expr
  | CompOp CompOp Expr Expr
  | CompAll CompOp Expr SubQuery
  | CompAny CompOp Expr SubQuery
  | InPred Expr QueryExpr
  | NotInPred Expr QueryExpr
  | Between Expr Expr Expr
  | NotBetween Expr Expr Expr
  | SoundsLike Expr Expr
  | Like Expr Expr
  | NotLike Expr Expr
  | Regexp Expr Expr
  | NotRegexp Expr Expr
  | BinOp BinOp Expr Expr
  -- | PlusInterval Expr IntervalExpr
  -- | MinusInterval Expr IntervalExpr
  | Literal Literal
  | IdentExpr Identifier
  | FCallExpr FCall
  | VarExpr String
  | UnaryOp UnaryOp Expr
  | RowExpr (Array Expr)
  | SubQueryExpr SubQuery
  | Exists SubQuery
  -- | IntervalExpr IntervalExpr

data Literal
  = StringLit String
  | IntLit Int
  | FloatLit Number

data Identifier
  = Identifier String
  | Qualified Alias String


newtype FCall = FCall {name :: String, args :: Array Expr}
