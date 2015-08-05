package futures

import java.util.concurrent.{ CountDownLatch, TimeUnit, Executors }

import mongo.MongoProgram.NamedThreadFactory
import org.apache.log4j.Logger
import org.specs2.mutable.Specification

import scala.concurrent._
import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.duration.FiniteDuration
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

  //Examples from  Learning Concurrent Programming in Scala
  type Cancellable[T] = (Promise[Unit], Future[T])
  def cancellable[T](b: Future[Unit] ⇒ T): Cancellable[T] = {
    val cancel = Promise[Unit]
    val f = Future {
      val r = b(cancel.future)
      if (!cancel.tryFailure(new Exception))
        throw new CancellationException
      r
    }
    (cancel, f)
  }

  "Cancellable" should {
    "have run" in {
      val (cancel, value) = cancellable { cancel ⇒
        var i = 0
        //poll 5 second for cancel
        while (i < 10) {
          if (cancel.isCompleted) throw new CancellationException
          Thread.sleep(500)
          println(s"$i: working")
          i += 1
        }
        "resulting value"
      }

      Thread.sleep(3000)
      //Thread.sleep(6000) > 5
      //b === false

      value.map(_ + "0").onComplete { _ ⇒ println("Eventually completed") }

      //If the promise has already been completed returns `false`, or `true` otherwise.
      val b = cancel.trySuccess()
      println("canceled " + b)
      b === true
    }
  }

  implicit class FutureOps[T](val self: Future[T]) {
    def or(that: Future[T]): Future[T] = {
      val p = Promise[T]
      self.onComplete { case x ⇒ p tryComplete x }
      that.onComplete { case y ⇒ p tryComplete y }
      p.future
    }
  }

  def timeout(t: Long): Future[Unit] = {
    import java.util._
    val timer = new Timer(true)
    val p = Promise[Unit]()
    timer.schedule(new TimerTask {
      override def run() = {
        p success ()
        timer.cancel()
      }
    }, t)
    p.future
  }

  "The nondeterminism of the future's or combinator" should {
    "have run" in {
      val EC = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(2))
      val time = new FiniteDuration(2, TimeUnit.SECONDS)
      //A future computation cannot be forcefully stopped
      val timeoutF = timeout(1000).map(_ ⇒ "timeout") or Future {
        Thread.sleep(1200)
        "completed"
      }(EC)

      val completedF = timeout(1000).map(_ ⇒ "timeout") or Future {
        Thread.sleep(800)
        "completed"
      }(EC)

      Await.result(timeoutF, time) === "timeout"
      Await.result(completedF, time) === "completed"
    }
  }

  /*
  http://quantifind.com/blog/2015/06/throttling-instantiations-of-scala-futures-1/
  val numWorkers = sys.runtime.availableProcessors
  val queueCapacity = 100
  implicit val ec = ExecutionContext.fromExecutorService(
    new ThreadPoolExecutor(
      numWorkers, numWorkers,
      0L, TimeUnit.SECONDS,
      new ArrayBlockingQueue[Runnable](queueCapacity) {
        override def offer(e: Runnable) = {
          put(e); // may block if waiting for empty room
          true
        }
      }
    )
  )
  */
}