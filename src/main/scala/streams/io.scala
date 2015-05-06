package streams

import scala.annotation.tailrec
import scalaz.{ -\/, \/-, \/ }
import scalaz.concurrent.Task
import scalaz.stream.Cause.{ EarlyCause, End, Kill }
import scalaz.stream.Process.{ Await, Emit, Halt, Step }
import scalaz.stream.{ Cause, Process }
import scala.language.higherKinds

object io {

  trait ChunkResource[I] {
    def request(n: Int): Seq[I]
    def chunk(n: Int): Process[Task, Seq[I]]
    def chunkN: Process[Task, Seq[I]]
  }

  def batcherHalter[I]: Cause ⇒ Process[Task, Seq[I]] = {
    case cause @ Cause.End ⇒
      Process.Halt(cause)
    case cause @ Cause.Kill ⇒
      Process.Halt(cause)
    case cause @ Cause.Error(ex) ⇒
      if (ex.getMessage == "IOF") Process.Halt(Cause.End)
      else Process.Halt(cause)
  }

  def chunkR[I](p: Process[Task, I]): Traversable[I] with ChunkResource[I] =
    new Traversable[I] with ChunkResource[I] {
      val P = Process
      var cur = p
      var buffer: Seq[I] = Nil

      def Try[F[_], A](p: ⇒ Process[F, A]): Process[F, A] =
        try p catch {
          case e: Throwable ⇒ P.fail(e)
        }

      override def chunkN: Process[Task, Seq[I]] = {
        def go(i: Int): Process[Task, Seq[I]] =
          P.await(Task.delay(request(i))) { seq ⇒
            if (seq.size < i) P.emit(seq) ++ P.halt else P.emit(seq) ++ go(i)
          }
        go(10)
      }

      override def chunk(n: Int): Process[Task, Seq[I]] =
        P.eval(Task.async(chunkCallback(n)))

      def chunkCallback(n: Int): (Throwable \/ Seq[I] ⇒ Unit) ⇒ Unit =
        cb ⇒ {
          if (n < 0) cb(-\/(new Exception("chunk size must be > 0, was: " + n)))

          val batch = request(n)
          if (batch.size == 0) cb(-\/(new Exception("IOF")))
          else cb(\/-(batch))
        }

      override def request(n: Int): Seq[I] =
        buffer match {
          case Nil ⇒
            fetchBuffer(n, 0)
            readN(n)
          case list ⇒
            if (list.size >= n)
              readN(n)
            else {
              fetchBuffer(n, 0)
              readN(n)
            }
        }

      private def readN(n: Int) = {
        val (r, rest) = buffer.splitAt(n)
        buffer = rest
        r
      }

      @tailrec
      def fetchBuffer(n: Int, acc: Int): Unit = {
        if (cur.isHalt) ()
        else if (n > acc) {
          step()
          if (buffer.size < n)
            fetchBuffer(n, acc + 1)
        } else ()
      }

      override def foreach[U](f: I ⇒ U): Unit = go(p, f)

      @tailrec
      def step(): Unit = {
        cur.step match {
          case h @ Halt(End | Kill)                ⇒ cur = h
          case h @ Halt(Cause.Error(e: Exception)) ⇒ cur = h
          case Step(Emit(as), cont) ⇒
            buffer = buffer ++ as
            cur = cont.continue
          case Step(Await(req, rcv), next) ⇒
            //Evaluate req
            val res = EarlyCause.fromTaskResult(req.attempt.run)
            //Transition to the next state
            cur = Try(rcv(res).run) +: next
            step()
        }
      }

      @tailrec
      def go[U](p: Process[Task, I], f: I ⇒ U): Unit = {
        p.step match {
          case Step(Emit(os), cont) ⇒
            os.foreach(f)
            go(cont.continue, f)
          case Step(Await(req, rcv), cont) ⇒
            val res = EarlyCause.fromTaskResult(req.attempt.run)
            go(Try(rcv(res).run) +: cont, f)
          case Halt(rsn) ⇒
        }
      }
    }
}