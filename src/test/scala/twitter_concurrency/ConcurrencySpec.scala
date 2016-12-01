package twitter_concurrency

import java.util.concurrent.atomic.{ AtomicReference, AtomicInteger }

import com.twitter.util.Witness
import org.apache.log4j.Logger
import org.specs2.mutable.Specification

import scala.annotation.tailrec

class ConcurrencySpec extends Specification {
  val logger = Logger.getLogger("twitter-concurrency")

  "Vars changes" should {
    "be observed correctly" in {
      import com.twitter.util.Var
      import com.twitter.util.Updatable
      val N = 100
      val refA, refB = Var(0)

      class Writer(n: Int, limit: Int, u: Updatable[Int], sleep: Long) extends Thread {
        @tailrec
        private def loop(index: Int): Unit = {
          if (index < limit) {
            u.update(index)
            logger.info(s"$n update local var with $index")
            Thread.sleep(sleep)
            loop(index + 1)
          } else ()
        }

        override def run() = loop(0)
      }

      //val both = refA.join(refB)
      val both = refA.flatMap(_ ⇒ refB)

      //val acc = new AtomicReference[(Int, Int)]((0, 0))
      val acc = new AtomicReference[Int](0)
      val w = Witness(acc)

      both.changes.register(w)

      val ac = new Writer(0, N, refA, 50)
      val bc = new Writer(1, N, refB, 70)

      ac.start()
      bc.start()
      ac.join()
      bc.join()

      acc.get === N - 1
      //acc.get === (N - 1, N - 1)
    }
  }
}
