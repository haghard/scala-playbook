package visitor

import org.specs2.mutable.Specification

class VisitorSpec extends Specification {

  "Patternmatching visitor" should {
    "eval" in {
      import visitor.patternmatching.{ Prod, Sum, Num }

      val expr = new Sum(new Num(1), new Prod(new Num(2), new Num(3)))
      patternmatching.eval(expr) should be equalTo 7
    }
  }

  "Visitor Pattern Generics" should {
    "eval" in {
      import VisitorPatternGenerics.{ Prod, Sum, Num }

      val expr = new Sum(new Num(1), new Prod(new Num(2), new Num(3)))
      VisitorPatternGenerics.eval(expr) should be equalTo 7
    }
  }

  "Visitor Pattern Typeclass" should {
    "eval" in {
      import VisitorPatternTypeclass._

      val expr: FakeModule#Expr = new FakeSum(new FakeNum(1),
        new FakeProd(new FakeNum(2), new FakeNum(3)))

      VisitorPatternTypeclass.eval[FakeModule](expr) should be equalTo 7
    }
  }

  "Visitor Pattern Catamorphism" should {
    "eval" in {
      import VisitorPatternCatamorphism._

      val expr = new Sum(new Num(1), new Prod(new Num(2), new Num(3)))
      VisitorPatternCatamorphism.eval(expr) should be equalTo 7
    }
  }

  "Visitor Pattern State" should {
    "eval" in {
      import VisitorPatternState._

      val expr = new Sum(new Num(1), new Prod(new Num(2), new Num(3)))
      VisitorPatternState.eval(expr) should be equalTo 7
    }
  }
}
