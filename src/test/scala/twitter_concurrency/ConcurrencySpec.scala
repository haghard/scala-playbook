package twitter_concurrency

import java.util.concurrent.atomic.{ AtomicReference, AtomicInteger }

import com.twitter.util.Witness
import org.apache.log4j.Logger
import org.specs2.mutable.Specification

import scala.annotation.tailrec

class ConcurrencySpec extends Specification {
  val logger = Logger.getLogger("twitter-concurrency")

  "Observe vars changes" should {
    "have observed only after pair update" in {
      import com.twitter.util.Var
      import com.twitter.util.Updatable
      val N = 100
      val a, b = Var(0)

      class Counter(n: Int, limit: Int, u: Updatable[Int], sleep: Long) extends Thread {
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

      val c = a.flatMap(_ ⇒ b)

      val acc = new AtomicReference[Int](0)
      val w = Witness(acc)

      c.changes.register(w)

      val ac = new Counter(0, N, a, 50)
      val bc = new Counter(1, N, b, 70)

      ac.start()
      bc.start()
      ac.join()
      bc.join()

      acc.get === N - 1
    }
  }
}
