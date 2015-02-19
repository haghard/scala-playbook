package object fpolimorphism {

  trait Entity[E <: Entity[E]] {
    self: E ⇒

    def create(): E
    def read(id: Long): Option[E]
    def update(f: E ⇒ E): E
    def delete(id: Long): Unit
  }

  class Apple extends Entity[Apple] {

    override def create(): Apple = ???

    override def update(f: (Apple) ⇒ Apple): Apple = ???

    override def delete(id: Long): Unit = ???

    override def read(id: Long): Option[Apple] = ???
  }

  class Orange extends Entity[Orange] {

    override def create(): Orange = ???

    override def update(f: (Orange) ⇒ Orange): Orange = ???

    override def delete(id: Long): Unit = ???

    override def read(id: Long): Option[Orange] = ???
  }

}
