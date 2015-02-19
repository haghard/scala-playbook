package visitor

/*
 * the Visitor Pattern with generics
 * the Visitor's type carries the kind of value that is computed
 */
object VisitorPatternGenerics {

  trait ExprVisitor[T] {
    def visit(num: Num): T
    def visit(sum: Sum): T
    def visit(prod: Prod): T
  }

  trait Expr {
    def accept[T](v: ExprVisitor[T]): T
  }

  case class Num(n: Int) extends Expr {
    def accept[T](v: ExprVisitor[T]): T = v.visit(this)
  }

  case class Sum(l: Expr, r: Expr) extends Expr {
    def accept[T](v: ExprVisitor[T]): T = v.visit(this)
  }

  case class Prod(l: Expr, r: Expr) extends Expr {
    def accept[T](v: ExprVisitor[T]): T = v.visit(this)
  }

  final class Eval extends ExprVisitor[Int] {
    def visit(num: Num): Int = num.n
    def visit(sum: Sum): Int = sum.l.accept(this) + sum.r.accept(this)
    def visit(prod: Prod): Int = prod.l.accept(this) * prod.r.accept(this)
  }

  def eval(expr: Expr): Int = expr.accept(new Eval)
}
