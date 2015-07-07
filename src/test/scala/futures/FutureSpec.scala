package futures

import java.util.concurrent.{ CountDownLatch, TimeUnit, Executors }

import mongo.MongoProgram.NamedThreadFactory
import org.apache.log4j.Logger
import org.specs2.mutable.Specification

import scala.concurrent._
import java.util.concurrent.atomic.AtomicReference
import scala.util.{ Failure, Success }

class FutureSpec extends Specification {

  private val logger = Logger.getLogger("futures")

  val E = Executors.newFixedThreadPool(4, new NamedThreadFactory("futures"))
  implicit val EC = ExecutionContext.fromExecutor(E)

  /**
   *
   * cb has Future[T] => T signature
   * because when that Future is completed, the code knows that it has been interrupted, this is important in the case where the
   * logic uses multiple threads underneath and you want to be able to propagate the interruption signal
   * across them all (Thread.interrupted() is only for a single Thread.)
   *
   * Only report successful cancellation if both interruption AND completion is successful
   * @param cb
   * @param ex
   * @tparam T
   * @return
   */
  def interruptableFuture[T](cb: Future[T] ⇒ T)(implicit ex: ExecutionContext): (Future[T], () ⇒ Boolean) = {
    val p = Promise[T]()
    val f = p.future
    val threadRef = new AtomicReference[Thread]
    p.tryCompleteWith(Future {
      val thread = Thread.currentThread
      threadRef.set(thread)
      try cb(f) finally {
        val wasInterrupted = (threadRef getAndSet null) ne thread
        logger.info(s"interrupted flag $wasInterrupted")
      }
    }(ex))

    (f, () ⇒ {
      Option(threadRef getAndSet null) foreach (th ⇒ {
        logger.info(s"Before interrupt $th")
        th.interrupt
      })
      p.tryFailure(new CancellationException)
    })
  }

  /**
   *
   * @param cancellableWork
   * @return
   */
  def delayed(cancellableWork: Future[Int]): Int = {
    try {
      val latch = new CountDownLatch(1)
      latch.await(2, TimeUnit.SECONDS)
    } catch {
      case e: Exception ⇒ logger.info("Future was interruptedException")
    }
    1
  }

  "InterruptableFuture" should {
    "have been cancelled" in {
      val (f, c) = interruptableFuture[Int](delayed)(EC)

      f.flatMap { start ⇒
        logger.info("start step1")
        Thread.sleep(1000)
        logger.info("end step1")
        Future(start * 2)
      }.map { r ⇒
        logger.info("start step2")
        Thread.sleep(1000)
        logger.info("end step2")
        r * 3
      }

      Thread.sleep(1500)
      c() === true
    }
  }

  "InterruptableFuture" should {
    "not have been cancelled" in {
      val latch = new CountDownLatch(1)
      val (f, c) = interruptableFuture[Int](delayed)(EC)

      f.flatMap { start ⇒
        logger.info("start step1")
        Thread.sleep(1000)
        logger.info("end step1")
        Future(start * 2)
      }.map { r ⇒
        logger.info("start step2")
        Thread.sleep(1000)
        logger.info("end step2")
        r * 3
      }.onComplete {
        case Success(r)  ⇒ { latch.countDown(); logger.info(s"read result $r"); }
        case Failure(ex) ⇒ { latch.countDown(); logger.info(ex.getMessage) }
      }

      Thread.sleep(2500)
      val wasInterrupted = c()

      latch.await(5, TimeUnit.SECONDS) === true
      wasInterrupted === false
    }
  }
}