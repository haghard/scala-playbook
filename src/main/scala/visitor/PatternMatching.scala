package visitor

object patternmatching {

  sealed trait Expr
  case class Num(n: Int) extends Expr
  case class Sum(l: Expr, r: Expr) extends Expr
  case class Prod(l: Expr, r: Expr) extends Expr

  def eval(e: Expr): Int = e match {
    case Num(n)     ⇒ n
    case Sum(l, r)  ⇒ eval(l) + eval(r)
    case Prod(l, r) ⇒ eval(l) * eval(r)
  }

}
