package modules

import org.specs2.mutable.Specification

class ModulesSpec extends Specification {

  "Program with var options" should {
    "run" in {
      object MainWithScalaOption extends Program[ScalaOption]
      object MainWithNullOption extends Program[NullOption]
      object MainWithJava8Option extends Program[Java8Option]
      object MainWithMyOption extends Program[MyOption]

      MainWithScalaOption.run()
      MainWithMyOption.run()
      MainWithNullOption.run()
      MainWithJava8Option.run()

      true should be equalTo true
    }
  }
}
