package visitor

object VisitorPatternCatamorphism {

  sealed trait Expr {
    /**
     *
     * @param fnum
     * @param fsum
     * @param fprod
     * @tparam T
     * @return
     */
    def accept[T](fnum: Num ⇒ T,
                  fsum: Sum ⇒ T,
                  fprod: Prod ⇒ T): T =
      this match {
        case num: Num   ⇒ fnum(num)
        case sum: Sum   ⇒ fsum(sum)
        case prod: Prod ⇒ fprod(prod)
      }
  }

  def eval(expr: Expr): Int = expr.accept(
    num ⇒ num.n,
    sum ⇒ eval(sum.l) + eval(sum.r),
    prod ⇒ eval(prod.l) * eval(prod.r))

  case class Num(n: Int) extends Expr
  case class Sum(l: Expr, r: Expr) extends Expr
  case class Prod(l: Expr, r: Expr) extends Expr
}
