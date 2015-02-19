package visitor

object VisitorPatternTypeclass {

  sealed trait Module {
    type Expr
    type Num <: Expr
    type Sum <: Expr
    type Prod <: Expr
  }

  trait Visitor[M <: Module] {

    trait NumCompanionObject { def unapply(num: M#Num): Option[Int] }

    val Num: NumCompanionObject

    trait SumCompanionObject { def unapply(sum: M#Sum): Option[(M#Expr, M#Expr)] }

    val Sum: SumCompanionObject

    trait ProdCompanionObject { def unapply(prod: M#Prod): Option[(M#Expr, M#Expr)] }

    val Prod: ProdCompanionObject

    def fold[T](expr: M#Expr)(fnum: M#Num ⇒ T, fsum: M#Sum ⇒ T, fprod: M#Prod ⇒ T): T
  }

  def eval[M <: Module](expr: M#Expr)(implicit visitor: Visitor[M]): Int = {
    import visitor._
    fold(expr)(
      { case Num(i) ⇒ i },
      { case Sum(l, r) ⇒ eval(l) + eval(r) },
      { case Prod(l, r) ⇒ eval(l) * eval(r) }
    )
  }

  sealed trait FakeExpr
  class FakeNum(val n: Int) extends FakeExpr
  class FakeSum(val l: FakeExpr, val r: FakeExpr) extends FakeExpr
  class FakeProd(val l: FakeExpr, val r: FakeExpr) extends FakeExpr

  sealed trait FakeModule extends Module {
    type Expr = FakeExpr
    type Num = FakeNum
    type Sum = FakeSum
    type Prod = FakeProd
  }

  implicit object VisitorForExt extends Visitor[FakeModule] {
    object Num extends NumCompanionObject {
      def unapply(num: FakeNum): Option[Int] = Some(num.n)
    }
    object Sum extends SumCompanionObject {
      def unapply(sum: FakeSum): Option[(FakeExpr, FakeExpr)] = Some((sum.l, sum.r))
    }
    object Prod extends ProdCompanionObject {
      def unapply(prod: FakeProd): Option[(FakeExpr, FakeExpr)] = Some((prod.l, prod.r))
    }

    def fold[T](expr: FakeExpr)(
      fnum: FakeNum ⇒ T,
      fsum: FakeSum ⇒ T,
      fprod: FakeProd ⇒ T): T = expr match {
      case num: FakeNum   ⇒ fnum(num)
      case sum: FakeSum   ⇒ fsum(sum)
      case prod: FakeProd ⇒ fprod(prod)
    }
  }
}
