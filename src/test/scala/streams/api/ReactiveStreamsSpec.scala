package streams.api

import java.util.concurrent.{ ExecutorService, TimeUnit, CountDownLatch }
import java.util.concurrent.atomic.AtomicReference
import org.scalatest.{ WordSpecLike, MustMatchers }

import scala.concurrent.SyncVar
import scala.concurrent.forkjoin.{ ForkJoinPool, ThreadLocalRandom }
import scalaz.concurrent.Task
import scalaz.stream._

class ReactiveStreamsSpec extends WordSpecLike with MustMatchers {
  import Procesess._

  implicit val E: ExecutorService =
    new ForkJoinPool(Runtime.getRuntime.availableProcessors() * 2)

  trait RandomRequestSubscriber[T] extends ScalazSubscriber[T] {
    override def resume() = ThreadLocalRandom.current().nextInt(1, 12)
  }

  trait OnNextBlowupSubscriber[T] extends ScalazSubscriber[T] {
    abstract override def onNext(t: T): Unit = {
      super.onNext(t)
      throw new Exception("onNext")
    }
  }

  trait OneElementSubscriber[T] extends ScalazSubscriber[T] {
    abstract override def onNext(t: T): Unit = {
      super.onNext(t)
      subscription.fold(())(_.cancel())
    }
  }

  "Publisher Subscriber with cancel" should {
    "do cancel" in {
      val source: Process[Task, Int] = naturals
      val latch = new CountDownLatch(1)

      ScalazPublisher.bounded[Int](source, 189)
        .subscribe(new ScalazSubscriber1[Int](latch, 10))

      assert(latch.await(3, TimeUnit.SECONDS))
    }
  }

  "Publisher Subscriber with OnNextBlowupSubscriber" should {
    "have thrown onNext" in {
      val sync = new SyncVar[Long]()
      val source: Process[Task, Int] = naturals
      val errors = new AtomicReference[Throwable]

      ScalazPublisher.bounded[Int](source, 25)
        .subscribe(new ScalazSubscriber[Int](11, sync, errors) with OnNextBlowupSubscriber[Int])

      sync.get
      errors.get().getMessage must be === "onNext"
    }
  }

  "Publisher and 3 Subscribers subscribed in different point of time" should {
    "run" in {
      val Size = 100
      val sync = new SyncVar[Long]()
      val errors = new AtomicReference[Throwable]
      val source: Process[Task, Int] =
        (naturals zip Process.repeatEval(Task.delay(Thread.sleep(ThreadLocalRandom.current().nextInt(100, 150))))).map(_._1)

      val ScalazSource = ScalazPublisher.bounded[Int](source, Size)

      ScalazSource.subscribe(new ScalazSubscriber[Int](10, sync, errors) with RandomRequestSubscriber[Int])
      Thread.sleep(200)

      val sync1 = new SyncVar[Long]()
      ScalazSource.subscribe(new ScalazSubscriber[Int](2, sync1, errors) with RandomRequestSubscriber[Int])
      Thread.sleep(200)

      val sync2 = new SyncVar[Long]()
      ScalazSource.subscribe(new ScalazSubscriber[Int](5, sync2, errors) with RandomRequestSubscriber[Int])

      val sync3 = new SyncVar[Long]()
      ScalazSource.subscribe(new ScalazSubscriber[Int](10, sync3, errors) with RandomRequestSubscriber[Int])

      sync1.get must be <= Size.toLong
      sync2.get must be <= Size.toLong
      sync3.get must be <= Size.toLong
      sync.get must be === Size.toLong
    }
  }
}