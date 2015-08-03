package common

import java.util.concurrent.ExecutorService

import scalaz.concurrent.Task

object KleisliSupport {
  import scalaz.Kleisli

  type Reader[T] = scalaz.ReaderT[Task, ExecutorService, T]
  type Delegated[A] = Kleisli[Task, ExecutorService, A]

  def delegate: Delegated[ExecutorService] = Kleisli.kleisli(e ⇒ Task.now(e))
  def reader: Reader[ExecutorService] = Kleisli.kleisli(e ⇒ Task.now(e))

  implicit class KleisliTask[T](val task: Task[T]) extends AnyVal {
    def kleisli: Delegated[T] = Kleisli.kleisli(_ ⇒ task)
    def kleisliR: Reader[T] = Kleisli.kleisli(_ ⇒ task)
  }
}
