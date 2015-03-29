package common

import java.util.concurrent.ExecutorService

import scalaz.concurrent.Task

object KleisliSupport {
  import scalaz.Kleisli
  type Delegated[A] = Kleisli[Task, ExecutorService, A]

  def delegate: Delegated[ExecutorService] = Kleisli.kleisli(e ⇒ Task.now(e))

  implicit class KleisliTask[A](val task: Task[A]) {
    def kleisli: Delegated[A] = Kleisli.kleisli(_ ⇒ task)
  }
}
