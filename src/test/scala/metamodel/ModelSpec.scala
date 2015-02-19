package metamodel

import org.specs2.mutable.Specification

class ModelSpec extends Specification {

  type F1 = MetaModel[T] forSome { type T <: Model[T] }

  "Metamodel" should {
    "speak" in {

      val list = List[F1](A1, A2, B1)
      list foreach { x ⇒ println(x.speak) }

      true should be equalTo true
    }
  }
}