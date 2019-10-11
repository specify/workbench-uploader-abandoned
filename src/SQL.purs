module SQL where

import Prelude

import Data.Foldable (intercalate)
import Data.Maybe (Maybe(..))
import Data.Newtype (class Newtype, wrap, unwrap)


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

data Relation = Table String | Query String

data SelectTerm = SelectTerm ScalarExpr | SelectAs String ScalarExpr

toSql :: SelectTerm -> String
toSql (SelectTerm s) = unwrap s
toSql (SelectAs alias s) = "(" <> (unwrap s) <> ") as " <> alias

instance showRelation :: Show Relation where
  show (Table s) = s
  show (Query s) = s

query :: Array SelectTerm -> FromExpr -> Array JoinExpr -> Maybe ScalarExpr -> Array ScalarExpr -> Relation
query = query' "select\n"

queryDistinct :: Array SelectTerm -> FromExpr -> Array JoinExpr -> Maybe ScalarExpr -> Array ScalarExpr -> Relation
queryDistinct = query' "select distinct\n"

query' :: String -> Array SelectTerm -> FromExpr -> Array JoinExpr -> Maybe ScalarExpr -> Array ScalarExpr -> Relation
query' selectType selectTerms fromExpr joinExprs whereExpr orderBy = Query $
  selectType <>
  (intercalate ",\n" $ map toSql $ selectTerms) <> "\n" <>
  (unwrap fromExpr) <> "\n" <>
  (intercalate "\n" $ map unwrap $ joinExprs) <> "\n" <>
  case whereExpr of
    Just expr -> "where\n" <> (unwrap expr)
    Nothing -> ""
  <> case orderBy of
    [] -> ""
    _ -> "\norder by " <> (intercalate ", " $ map unwrap $ orderBy)

join' :: JoinType -> Relation -> Alias -> Maybe ScalarExpr -> JoinExpr
join' joinType source alias onExpr = wrap case source of
  Table name -> (unwrap joinType) <>  " " <> name <> " " <> (unwrap alias) <> on
  Query str -> (unwrap joinType) <> " (\n" <> str <> "\n) " <> (unwrap alias) <> on
  where on = case onExpr of
          Just expr -> " on\n" <> (unwrap expr)
          Nothing -> "\n"

join :: Relation -> Alias -> Maybe ScalarExpr -> JoinExpr
join = join' (wrap "join")

leftJoin :: Relation -> Alias -> Maybe ScalarExpr -> JoinExpr
leftJoin = join' (wrap "left join")

rightJoin :: Relation -> Alias -> Maybe ScalarExpr -> JoinExpr
rightJoin = join' (wrap "right join")

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

as :: Relation -> Alias -> FromExpr
as select alias = wrap case select of
  Table t -> "from " <> t <> " " <> unwrap alias
  Query q -> "from (\n" <> q <> "\n) " <> unwrap alias

from :: Relation -> FromExpr
from (Table s) = wrap $ "from " <> s
from (Query q) = wrap $ "from (\n" <> q <> "\n) " -- some alias may be required

star :: Alias -> SelectTerm
star (Alias a) = SelectTerm $ wrap $ a <> ".*"

notIn :: ScalarExpr -> Array ScalarExpr -> Maybe ScalarExpr
notIn value [] = Nothing
notIn value values = Just $ wrap $ unwrap value <> " not in (" <> intercalate ", " (map unwrap values) <> ")"

notInSubQuery :: ScalarExpr -> Relation -> ScalarExpr
notInSubQuery value relation = wrap $ unwrap value <> " not in (" <> show relation <> ")"

insertFrom :: Relation -> Array String -> String -> String
insertFrom select columns table = "insert into " <> table <> "(\n" <> (intercalate ",\n" columns) <> "\n) " <> show select

insertValues :: Array (Array ScalarExpr) -> Array String -> String -> String
insertValues values columns table =
  "insert into " <> table <> "(\n" <> (intercalate ",\n" columns) <> "\n) values \n" <>
  (intercalate ",\n" $ map valuesExpr values)
  where
    valuesExpr vs = "(" <> (intercalate ", " $ map unwrap vs) <> ")"

stringLiteral :: String -> ScalarExpr
stringLiteral s = wrap $ "'" <> s <> "'"

intLiteral :: Int -> ScalarExpr
intLiteral n = wrap $ show n

strToDate :: ScalarExpr -> ScalarExpr -> ScalarExpr
strToDate str format = wrap $ "str_to_date(" <> unwrap str <> ", " <> unwrap format <> ")"

setUserVar :: String -> ScalarExpr -> String
setUserVar varName value = "set @" <> varName <> " = " <> unwrap value

varExpr :: String -> ScalarExpr
varExpr varName = ScalarExpr $ "@" <> varName

tuple :: Array ScalarExpr -> ScalarExpr
tuple exprs = ScalarExpr $ "(" <> (intercalate ", " $ map unwrap exprs) <> ")"

isNotDistinctFrom :: ScalarExpr -> ScalarExpr -> ScalarExpr
isNotDistinctFrom x y = ScalarExpr $ (unwrap x) <> " <=> " <> (unwrap y)

infixl 8 isNotDistinctFrom as <=>

assignVar :: ScalarExpr -> ScalarExpr -> ScalarExpr
assignVar x y = ScalarExpr $ (unwrap x) <> " := " <> (unwrap y)

infixl 8 assignVar as :=
