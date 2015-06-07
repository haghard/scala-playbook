package modules

import org.specs2.mutable.Specification

class ModulesSpec extends Specification {

  "Program parametrized by Options subtype" should {
    "run" in {

      object MainWithScalaOption extends Program[ScalaOption]
      object MainWithScalaOption2 extends Program[ScalaOption2]
      object MainWithNullOption extends Program[NullableOption]
      object MainWithJava8Option extends Program[Java8Option]

      MainWithScalaOption.run(scala.Option(12.56))
      MainWithScalaOption2.run(scala0.None)
      MainWithNullOption.run(null)
      MainWithJava8Option.run(java.util.Optional.of('c'))

      true should be equalTo true
    }
  }
}
