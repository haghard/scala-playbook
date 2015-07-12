package streams.api

import scalaz.concurrent.Task
import scalaz.stream.Process

object Procesess {
  def naturals: Process[Task, Int] = {
    def go(i: Int): Process[Task, Int] =
      Process.await(Task.delay(i))(i ⇒ Process.emit(i) ++ go(i + 1))
    go(0)
  }
}
