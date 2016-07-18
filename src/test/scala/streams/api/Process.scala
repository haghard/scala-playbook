package streams.api

object Process {
  import scalaz.concurrent.Task

  def naturals: scalaz.stream.Process[Task, Int] = {
    def go(i: Int): scalaz.stream.Process[Task, Int] =
      scalaz.stream.Process.await(Task.delay(i))(i ⇒ scalaz.stream.Process.emit(i) ++ go(i + 1))
    go(0)
  }
}
