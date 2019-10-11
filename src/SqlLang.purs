module SqlLang where

import Prelude

import Control.Monad.Writer (Writer, execWriter, tell)
import Data.Array (intercalate, uncons)
import Data.Maybe (Maybe(..))
import Data.NonEmpty (NonEmpty(..))
import Data.Traversable (for_, sequence, sequence_)



data Statement
  = QueryStatement QueryExpr
  -- | InsertStatement String

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
  | IsNotDistinctFrom Expr
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

type StatementM = Writer String Unit

render :: StatementM -> String
render s = execWriter s

renderStatement :: Statement -> StatementM
renderStatement (QueryStatement q) = do
  renderQuery q
  tell ";\n"

nl :: StatementM
nl = tell "\n"

sp :: StatementM
sp = tell " "

cs :: StatementM
cs = tell ", "

renderQuery :: QueryExpr -> StatementM
renderQuery (QueryExpr q) = case uncons q.selectTerms of
  Nothing -> tell mempty
  Just {head, tail} -> do
    renderSelectType q.selectType
    sp
    renderSelectTerms (NonEmpty head tail)
    nl
    renderFrom q.from
    nl
    renderWhere q.where_
    nl
    renderOrderBy q.orderBy
  -- nl
  -- renderLimit q.limit

renderOrderBy :: Array OrderTerm -> StatementM
renderOrderBy [] = tell mempty
renderOrderBy terms = do
    tell "order by"
    sp
    commaSeparated renderOrderTerm terms
  where
    renderOrderTerm (Ascending x) = renderExpr x >>= \_ -> tell " asc"
    renderOrderTerm (Descending x) = renderExpr x >>= \_ -> tell " desc"

renderWhere :: Maybe Expr -> StatementM
renderWhere Nothing = tell mempty
renderWhere (Just x) = do
  tell "where "
  renderExpr x

renderFrom :: NonEmpty Array TableRef -> StatementM
renderFrom (NonEmpty tr trs) = do
  tell "from "
  case trs of
    [] -> renderTableRef tr
    otherwise -> inParens do
      renderTableRef tr
      renderTableRefs trs
  where
    renderTableRefs = uncons >>> case _ of
      Nothing -> tell ""
      Just {head, tail} -> do
        cs
        renderTableRef head
        renderTableRefs tail

renderTableRef :: TableRef -> StatementM
renderTableRef (TableFactor tf) = renderTableFactor tf
renderTableRef (JoinedTable jt) = renderJoinedTable jt

renderJoinedTable :: JoinedTable -> StatementM
renderJoinedTable (InnerJoin l r o) = do
  renderTableRef l
  tell " join "
  renderTableRef r
  case o of
    Nothing -> tell mempty
    Just on -> renderJoinSpec on

renderJoinedTable (LeftJoin l r o) = do
  renderTableRef l
  tell " left join "
  renderTableRef r
  renderJoinSpec o

renderJoinedTable (RightJoin _ _ _) = notImplemented
renderJoinedTable (NaturalLeftJoin _ _) = notImplemented
renderJoinedTable (NaturalRightJoin _ _) = notImplemented



renderJoinSpec :: JoinSpec -> StatementM
renderJoinSpec (JoinOn expr) = do
  tell " on "
  renderExpr expr

renderJoinSpec (JoinUsing cols) = case uncons cols of
  Nothing -> tell mempty
  Just {head: ColumnName n, tail} -> do
    tell " using "
    inParens do
      tell n
      renderCols tail
  where
    renderCols = uncons >>> case _ of
      Nothing -> tell mempty
      Just {head: ColumnName n, tail} -> do
        cs
        tell n
        renderCols tail

renderTableFactor :: TableFactor -> StatementM
renderTableFactor (Table {name: TableName name, as}) = do
  tell name
  case as of
    Just (Alias a) -> do
      tell " as "
      tell a
    Nothing ->
      tell mempty

renderTableFactor (SubQuery _) = notImplemented
renderTableFactor (Tables _) = notImplemented
renderTableFactor Dual = tell "dual"

renderSelectType :: SelectType -> StatementM
renderSelectType SelectAll = tell "select"
renderSelectType SelectDistinct = tell "select distinct"
renderSelectType SelectDistinctRow = tell "select distinctrow"

renderSelectTerms :: NonEmpty Array SelectTerm -> StatementM
renderSelectTerms (NonEmpty t ts) = do
  renderSelectTerm t
  render' ts
  where
    render' = uncons >>> case _ of
      Nothing -> tell mempty
      Just {head, tail} -> do
        cs
        renderSelectTerm head
        render' tail

renderSelectTerm :: SelectTerm -> StatementM
renderSelectTerm Star = tell "*"
renderSelectTerm (SelectTerm expr) = renderExpr expr
renderSelectTerm (SelectAs as expr) = do
  renderExpr expr
  sp
  tell "as"
  sp
  tell as

inParens :: StatementM -> StatementM
inParens s = do
  tell "("
  s
  tell ")"

renderExpr :: Expr -> StatementM
renderExpr (OrExpr x y) = inParens do
  renderExpr x
  sp
  tell "or"
  sp
  renderExpr y

renderExpr (XorExpr x y) = inParens do
  renderExpr x
  sp
  tell "xor"
  sp
  renderExpr y

renderExpr (AndExpr x y) = inParens do
  renderExpr x
  sp
  tell "and"
  sp
  renderExpr y

renderExpr (NotExpr x) = inParens do
  tell "not"
  sp
  renderExpr x

renderExpr (IsTrueExpr p) = inParens do
  renderExpr p
  sp
  tell "is true"

renderExpr (IsFalseExpr p) = inParens do
  renderExpr p
  sp
  tell "is false"

renderExpr (IsUnknownExpr p) = inParens do
  renderExpr p
  sp
  tell "is unknown"

renderExpr (IsNull p) = inParens do
  renderExpr p
  sp
  tell "is null"

renderExpr (IsNotNull p) = inParens do
  renderExpr p
  sp
  tell "is not null"

renderExpr (IsNotDistinctFrom p) = inParens do
  renderExpr p
  sp
  tell "<=>"
  sp
  renderExpr p

renderExpr (CompOp op x y) = inParens do
  renderExpr x
  sp
  renderCompOp op
  sp
  renderExpr y

renderExpr (CompAll _ _ _) = notImplemented
renderExpr (CompAny _ _ _) = notImplemented

renderExpr (InPred x sq) = inParens do
  renderExpr x
  sp
  tell "in"
  sp
  inParens do
    nl
    renderQuery sq
    nl

renderExpr (NotInPred x sq) = inParens do
  renderExpr x
  sp
  tell "not in"
  sp
  inParens do
    nl
    renderQuery sq
    nl


renderExpr (Between _ _ _) = notImplemented
renderExpr (NotBetween _ _ _) = notImplemented
renderExpr (SoundsLike _ _) = notImplemented
renderExpr (Like _ _) = notImplemented
renderExpr (NotLike _ _) = notImplemented
renderExpr (Regexp _ _) = notImplemented
renderExpr (NotRegexp _ _) = notImplemented


renderExpr (BinOp op x y) = inParens do
  renderExpr x
  sp
  renderBinOp op
  sp
  renderExpr y


renderExpr (Literal l) = renderLiteral l
renderExpr (IdentExpr i) = renderIdentifier i

renderExpr (FCallExpr (FCall {name, args})) = do
  tell name
  inParens $ commaSeparated renderExpr args

renderExpr (VarExpr s) = notImplemented
renderExpr (UnaryOp _ _) = notImplemented
renderExpr (RowExpr _) = notImplemented
renderExpr (SubQueryExpr _) = notImplemented
renderExpr (Exists _) = notImplemented


commaSeparated :: forall a. (a -> StatementM) -> Array a -> StatementM
commaSeparated render xs = sequence_ $ intercalate [cs] $ map (render >>> pure) xs

renderBinOp :: BinOp -> StatementM
renderBinOp AndOp = tell "&"
renderBinOp OrOp = tell "|"
renderBinOp PlusOp = tell "+"
renderBinOp MinusOp = tell "-"

renderCompOp :: CompOp -> StatementM
renderCompOp E_Op = tell "="
renderCompOp GE_Op = tell ">="
renderCompOp G_Op = tell ">"
renderCompOp LE_Op = tell "<="
renderCompOp L_Op = tell "<"
renderCompOp NE_Op = tell "<>"

renderLiteral :: Literal -> StatementM
renderLiteral (IntLit i) = tell $ show i
renderLiteral (FloatLit n) = tell $ show n
renderLiteral (StringLit s) = do
  tell "'"
  tell $ escapeString s
  tell "'"

renderIdentifier :: Identifier -> StatementM
renderIdentifier (Identifier s) = tell $ escapeIdentifier s
renderIdentifier (Qualified (Alias a) s) = do
  tell $ escapeIdentifier a
  tell "."
  tell $ escapeIdentifier s

escapeString :: String -> String
escapeString = identity -- todo

escapeIdentifier :: String -> String
escapeIdentifier = identity -- todo

-- renderScalarExpr :: ScalarExpr -> StatementM
-- renderScalarExpr (StringLiteral s) = "'" <> s <> "'"
-- renderScalarExpr (IntLiteral i) = show i
-- renderScalarExpr (VarDeRef v) = "@" <> v
-- renderScalarExpr (BinOp op x y) = renderBinOp op x y
-- renderScalarExpr (UnaryOp op x) = renderUnaryOp op x
-- renderScalarExpr (DotRef source key) = source <> "." <> key

-- renderUnaryOp :: UnaryOp -> ScalarExpr -> String
-- renderUnaryOp op x = case op of
--   NotOp -> "not (" <> renderScalarExpr x <> ")"
--   IsNullOp -> "is null (" <> renderScalarExpr x <> ")"

-- renderBinOp :: BinOp -> ScalarExpr -> ScalarExpr -> String
-- renderBinOp op x y = case op of
--   EqualOp -> x' <> " = " <> y'
--   AndOp -> x' <> " and " <> y'
--   OrOp -> x' <> " or " <> y'
--   where
--     x' = "(" <> renderScalarExpr x <> ")"
--     y' = "(" <> renderScalarExpr y <> ")"

-- renderFromClause :: FromClause -> String
-- renderFromClause (FromClause {as, relation}) = case relation of
--  (TableRef name) -> ""
--  (QueryLiteral queryExpr) -> ""

-- renderJoinClause :: JoinClause -> String
-- renderJoinClause (JoinClause j) = case j.on of
--   Just expr -> j.joinType <> " " <> source <> " " <> as <> " on " <> renderScalarExpr expr
--   Nothing -> j.joinType <> " " <> source <> " " <> as
--   where source = case j.source of
--           TableRef name -> name
--           QueryLiteral expr -> "(\n" <> renderQuery expr <> "\n)"
--         as = fromMaybe "" j.as

-- renderWhere :: Maybe ScalarExpr -> String
-- renderWhere (Just expr) = "\nwhere " <> renderScalarExpr expr
-- renderWhere Nothing = ""

notImplemented :: StatementM
notImplemented = tell "<not implemented>"

