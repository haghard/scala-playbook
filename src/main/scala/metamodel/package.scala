package object metamodel {

  /**
   *
   * Assistance with generic type parameter
   *
   * https://groups.google.com/forum/#!topic/scala-user/QIRQmQJkA8Q
   * https://gist.github.com/propensive/f16276b4689f5b1cd8a2
   */
  import language.existentials

  trait Model[T <: Model[T]] {
    self: T ⇒
  }

  trait MetaModel[T <: Model[T]] {
    def speak: String
  }

  case class Model1(a: Int) extends Model[Model1]

  case class Model2(a: Int, b: String) extends Model[Model2]

  object A1 extends MetaModel[Model1] { def speak = s"Model1, A1" }

  object B1 extends MetaModel[Model1] { def speak = "Model1, B1" }

  object A2 extends MetaModel[Model2] { def speak = "Model2, A2" }

}
