module SqlSmart where

import Prelude
import SqlLang

import Data.Array (snoc, uncons)
import Data.Maybe (Maybe(..))
import Data.NonEmpty (NonEmpty(..))


class Fromable a where
  toTableRef :: a -> TableRef

instance fromableTableRef :: Fromable TableRef where
  toTableRef = identity

instance fromableTableFactor :: Fromable TableFactor where
  toTableRef = TableFactor

distinct :: QueryExpr -> QueryExpr
distinct (QueryExpr q) = QueryExpr $ q { selectType = SelectDistinct }

query :: Array SelectTerm -> QueryExpr
query terms =
  QueryExpr { selectType : SelectAll
            , selectTerms : terms
            , from : NonEmpty (TableFactor Dual) []
            , where_ : Nothing
            , orderBy : []
            , limit : Nothing
            }

queryDistinct :: Array SelectTerm -> QueryExpr
queryDistinct = query >>> distinct

from :: QueryExpr -> Array TableRef -> QueryExpr
from (QueryExpr q) fs = QueryExpr $ q { from = from' }
  where
    from' = case uncons fs of
      Just {head, tail} -> NonEmpty head tail
      Nothing -> NonEmpty (TableFactor Dual) []

join :: TableRef -> TableRef -> TableRef
join t f = JoinedTable $ InnerJoin t f Nothing

leftJoin :: TableRef -> TableRef -> TableRef
leftJoin t u = JoinedTable $ LeftJoin t u (JoinOn $ intLit 0)

on :: TableRef -> Expr -> TableRef
on (JoinedTable jt) expr = JoinedTable $case jt of
  InnerJoin l r o -> InnerJoin l r $ Just $ JoinOn expr
  LeftJoin l r o ->  LeftJoin l r $ JoinOn expr
  RightJoin l r o ->  RightJoin l r $ JoinOn expr
  NaturalLeftJoin l r -> NaturalLeftJoin l r
  NaturalRightJoin l r ->  NaturalRightJoin l r

on tf _ = tf

table :: String -> TableRef
table name = TableFactor $ Table {name: TableName name, as: Nothing}

as :: TableRef -> Alias -> TableRef
as t a = case t of
  TableFactor tf -> TableFactor $ tfAs tf a
  JoinedTable jt -> JoinedTable $ jtAs jt a
  where
    tfAs :: TableFactor -> Alias -> TableFactor
    tfAs tf a = case tf of
      Table t -> Table $ t { as = Just a }
      SubQuery s -> SubQuery $ s { as = a}
      Tables ts -> Tables ts
      Dual -> Dual
    jtAs :: JoinedTable -> Alias -> JoinedTable
    jtAs jt a = case jt of
      InnerJoin l r o -> InnerJoin l (r `as` a) o
      LeftJoin l r o -> LeftJoin l (r `as` a) o
      RightJoin l r o -> RightJoin l (r `as` a) o
      NaturalLeftJoin l r -> NaturalLeftJoin l (r `as` a)
      NaturalRightJoin l r -> NaturalRightJoin l (r `as` a)


suchThat :: QueryExpr -> Expr -> QueryExpr
suchThat (QueryExpr q) expr = QueryExpr $ q { where_ = Just expr }

orderBy :: QueryExpr -> Array OrderTerm -> QueryExpr
orderBy (QueryExpr q) terms = QueryExpr $ q { orderBy = terms }

asc :: Expr -> OrderTerm
asc = Ascending

desc :: Expr -> OrderTerm
desc = Descending

floatLit :: Number -> Expr
floatLit n = Literal $ FloatLit n

intLit :: Int -> Expr
intLit i = Literal $ IntLit i

stringLit :: String -> Expr
stringLit s = Literal $ StringLit s

select :: Expr -> SelectTerm
select = SelectTerm

selectAs :: String -> Expr -> SelectTerm
selectAs = SelectAs

projectFrom :: Alias -> String -> Expr
projectFrom alias name = IdentExpr $ Qualified alias name

project :: String -> Expr
project name = IdentExpr $ Identifier name

infixl 9 projectFrom as ..

and :: Expr -> Expr -> Expr
and = AndExpr

equal :: Expr -> Expr -> Expr
equal x y = CompOp E_Op x y

nullIf :: Expr -> Expr -> Expr
nullIf x y = FCallExpr $ FCall {name: "nullif", args: [x, y]}

strToDate :: Expr -> Expr -> Expr
strToDate format value = FCallExpr $ FCall {name: "strtodate", args: [format, value]}

plus :: Expr -> Expr -> Expr
plus x y = BinOp PlusOp x y

notInSubQuery :: Expr -> QueryExpr -> Expr
notInSubQuery x sq = NotInPred x sq
