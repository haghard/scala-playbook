package modules

import org.specs2.mutable.Specification

class ModulesSpec extends Specification {

  "Abstract Algebraic Data Types with abstract types and a catamorphism" should {
    import OptionType._

    "run with GuavaOption" in {
      import com.google.common.base.{ Optional ⇒ GOptional }
      val p = Program[GuavaOption]
      p.run(GOptional.absent[Int]) === None
      p.run(GOptional.of(45)) === Some
      p.run(GOptional.absent[Int].or(GOptional.of(45))) === Some
    }

    "ScalaOption" in {
      val p = Program[ScalaOption]
      p.run(Option(12)) === Some
      p.run(scala.None) === None
      p.run(Option(67.5).map(_ * 2)) === Some
    }

    "Custom ScalaOption" in {
      val p = Program[ScalaOption2]
      p.run(scala0.Some(12)) === Some
      p.run(scala0.None) === None
      p.run(scala0.Some("Aaa")) === Some
    }

    "Java8Option" in {
      import java.util.{ Optional ⇒ JOptional }
      val p = Program[Java8Option]
      def f = new java.util.function.Function[Double, Double]() {
        def apply(a: Double) = a * 5
      }
      p.run(JOptional.of(123.67).map(f)) === Some
      p.run(JOptional.empty) === None
    }

    "NullableOption" in {
      val p = Program[NullableOption]
      p.run(null) === None
      p.run("12") === Some
    }
  }
}