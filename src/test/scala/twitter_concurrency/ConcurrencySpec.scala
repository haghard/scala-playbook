package twitter_concurrency

import java.util.concurrent.atomic.AtomicInteger

import org.apache.log4j.Logger
import org.specs2.mutable.Specification

class ConcurrencySpec extends Specification {
  val logger = Logger.getLogger("twitter-concurrency")

  "Observe vars changes" should {
    "have observed only after pair update" in {
      import com.twitter.util.Var
      import com.twitter.util.Updatable
      val N = 100
      val a, b = Var(0)

      class Counter(n: Int, limit: Int, u: Updatable[Int], sleep: Long) extends Thread {
        override def run() {
          var i = 1
          while (i < limit) {
            u.update(i)
            logger.info(s"$n update local var $i")
            i += 1
            Thread.sleep(sleep)
          }
        }
      }

      val c = a.flatMap(_ ⇒ b)
      val acc = new AtomicInteger(-1)
      c.observe { i ⇒
        logger.info(s"update notification $i")
        assert(i === acc.get() + 1)
        acc.set(i)
      }

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
