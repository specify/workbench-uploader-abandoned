module SQL where

import Prelude

import Data.Foldable (intercalate)
import Data.Maybe (Maybe(..))
import Data.Newtype (class Newtype, wrap, unwrap)
import Data.Traversable (class Traversable)

newtype ScalarExpr = ScalarExpr String
derive instance newtypeScalarExpr :: Newtype ScalarExpr _

newtype JoinExpr = JoinExpr String
derive instance newtypeJoinExpr :: Newtype JoinExpr _

newtype FromExpr = FromExpr String
derive instance newtypeFromExpr :: Newtype FromExpr _

newtype JoinType = JoinType String
derive instance newtypeJoinType :: Newtype JoinType _

newtype Alias = Alias String
derive instance newtypeAlias :: Newtype Alias _

data SelectExpr = Table String | Query String

data SelectTerm = SelectTerm ScalarExpr | SelectAs String ScalarExpr

toSql :: SelectTerm -> String
toSql (SelectTerm s) = unwrap s
toSql (SelectAs alias s) = "(" <> (unwrap s) <> ") as  " <> alias

instance showSelectExpr :: Show SelectExpr where
  show (Table s) = s
  show (Query s) = s

query :: forall f g. Traversable f => Traversable g => f SelectTerm -> FromExpr -> g JoinExpr -> Maybe ScalarExpr -> SelectExpr
query selectTerms fromExpr joinExprs whereExpr = Query $
  "select\n" <>
  (intercalate ",\n" $ map toSql $ selectTerms) <> "\n" <>
  (unwrap fromExpr) <> "\n" <>
  (intercalate "\n" $ map unwrap $ joinExprs) <> "\n" <>
  case whereExpr of
    Just expr -> "where\n" <> (unwrap expr)
    Nothing -> ""

join' :: JoinType -> SelectExpr -> Alias -> Maybe ScalarExpr -> JoinExpr
join' joinType source alias onExpr = wrap case source of
  Table name -> (unwrap joinType) <>  " " <> name <> " " <> (unwrap alias) <> on
  Query str -> (unwrap joinType) <> " (\n" <> str <> "\n) " <> (unwrap alias) <> on
  where on = case onExpr of
          Just expr -> " on\n" <> (unwrap expr)
          Nothing -> "\n"

join :: SelectExpr -> Alias -> Maybe ScalarExpr -> JoinExpr
join = join' (wrap "join")

leftJoin :: SelectExpr -> Alias -> Maybe ScalarExpr -> JoinExpr
leftJoin = join' (wrap "left join")

and :: ScalarExpr -> ScalarExpr -> ScalarExpr
and a b = wrap $ "(" <> (unwrap a) <> ") and\n(" <> (unwrap b) <> ")"

or :: ScalarExpr -> ScalarExpr -> ScalarExpr
or a b = wrap $ "(" <> (unwrap a) <> ") or\n(" <> (unwrap b) <> ")"

getValue :: Alias -> String -> ScalarExpr
getValue alias name = wrap $ (unwrap alias) <> "." <> name

infixl 9 getValue as ..

equal :: ScalarExpr -> ScalarExpr -> ScalarExpr
equal a b = wrap $ (unwrap a) <> " = " <> (unwrap b)

isNull :: ScalarExpr -> ScalarExpr
isNull x = wrap $ (unwrap x) <> " is null"

nullIf :: ScalarExpr -> ScalarExpr -> ScalarExpr
nullIf x y = wrap $ "nullif(" <> (unwrap x) <> ", " <> (unwrap y) <>")"

plus :: ScalarExpr -> ScalarExpr -> ScalarExpr
plus x y = wrap $ "(" <> (unwrap x) <> " + " <> (unwrap y) <> ")"
