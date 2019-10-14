module Data.SQL.Render where

import Data.SQL.Syntax
import Prelude

import Control.Monad.Writer (Writer, execWriter, tell)
import Data.Array (intercalate, uncons)
import Data.Foldable (for_)
import Data.Maybe (Maybe(..))
import Data.NonEmpty (NonEmpty(..))
import Data.Traversable (sequence_)
import Debug.Trace (traceM)


type StatementM = Writer String Unit

notImplemented :: forall a. a -> StatementM
notImplemented a = do
  tell "<not implemented>"
  traceM a

render :: StatementM -> String
render s = execWriter s

separatedBy :: forall a. StatementM -> (a -> StatementM) -> Array a -> StatementM
separatedBy separator render xs =
  sequence_ $ intercalate [separator] $ map (render >>> pure) xs

commaSeparated :: forall a. (a -> StatementM) -> Array a -> StatementM
commaSeparated = separatedBy cs

renderStatement :: Statement -> StatementM
renderStatement (QueryStatement q) = do
  renderQuery q
  tell ";\n"

renderStatement (InsertValues i) = do
  tell "insert into"
  sp
  renderTableName i.table
  sp
  inParens $ commaSeparated renderColumnName i.columns
  nl
  tell "values"
  nl
  separatedBy cnl (inParens <<< commaSeparated renderExpr) i.values
  tell ";\n"

renderStatement (InsertFrom i) = do
  tell "insert into"
  sp
  renderTableName i.table
  sp
  inParens $ commaSeparated renderColumnName i.columns
  nl
  renderQuery i.query
  tell ";\n"

renderTableName :: TableName -> StatementM
renderTableName (TableName n) = tell $ escapeIdentifier n

renderColumnName :: ColumnName -> StatementM
renderColumnName (ColumnName n) = tell $ escapeIdentifier n

nl :: StatementM
nl = tell "\n"

sp :: StatementM
sp = tell " "

cs :: StatementM
cs = tell ", "

cnl :: StatementM
cnl = tell ",\n"

renderQuery :: QueryExpr -> StatementM
renderQuery (QueryExpr q) = case uncons q.selectTerms of
  Nothing -> tell mempty
  Just {head, tail} -> do
    renderSelectType q.selectType
    sp
    renderSelectTerms (NonEmpty head tail)
    renderFrom q.from
    renderWhere q.where_
    renderOrderBy q.orderBy
  -- renderLimit q.limit

renderOrderBy :: Array OrderTerm -> StatementM
renderOrderBy [] = tell mempty
renderOrderBy terms = do
  nl
  tell "order by"
  sp
  commaSeparated renderOrderTerm terms
  where
    renderOrderTerm (Ascending x) = renderExpr x >>= \_ -> tell " asc"
    renderOrderTerm (Descending x) = renderExpr x >>= \_ -> tell " desc"

renderWhere :: Maybe Expr -> StatementM
renderWhere Nothing = tell mempty
renderWhere (Just x) = do
  nl
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

renderJoinedTable p@(RightJoin _ _ _) = notImplemented p
renderJoinedTable p@(NaturalLeftJoin _ _) = notImplemented p
renderJoinedTable p@(NaturalRightJoin _ _) = notImplemented p



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

renderTableFactor (SubQuery {query, as: Alias a, columns}) = do
  inParens do
    nl
    renderQuery query
    nl
  tell " as "
  tell a

renderTableFactor p@(Tables _) = notImplemented p
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

renderExpr (IsNotDistinctFrom x y) = inParens do
  renderExpr x
  sp
  tell "<=>"
  sp
  renderExpr y

renderExpr (CompOp op x y) = inParens do
  renderExpr x
  sp
  renderCompOp op
  sp
  renderExpr y

renderExpr p@(CompAll _ _ _) = notImplemented p
renderExpr p@(CompAny _ _ _) = notImplemented p

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


renderExpr p@(Between _ _ _) = notImplemented p
renderExpr p@(NotBetween _ _ _) = notImplemented p
renderExpr p@(SoundsLike _ _) = notImplemented p
renderExpr p@(Like _ _) = notImplemented p
renderExpr p@(NotLike _ _) = notImplemented p
renderExpr p@(Regexp _ _) = notImplemented p
renderExpr p@(NotRegexp _ _) = notImplemented p


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

renderExpr (VarExpr s) = tell $ "@" <> escapeIdentifier s

renderExpr p@(UnaryOp _ _) = notImplemented p

renderExpr (RowExpr values) = inParens $ commaSeparated renderExpr values

renderExpr (SubQueryExpr {query, as: Alias a, columns}) = do
  inParens do
    nl
    renderQuery query
    nl
  tell " as "
  tell a


renderExpr p@(Exists _) = notImplemented p


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


