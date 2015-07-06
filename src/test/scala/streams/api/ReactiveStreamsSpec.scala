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

  trait RandomRequestSubscriber[T] extends SyncProcessSubscriber[T] {
    override def updateBufferSize() = ThreadLocalRandom.current().nextInt(1, 12)
  }

  trait OnNextBlowupSubscriber[T] extends SyncProcessSubscriber[T] {
    abstract override def onNext(t: T): Unit = {
      super.onNext(t)
      throw new Exception("onNext")
    }
  }

  trait OneElementSubscriber[T] extends SyncProcessSubscriber[T] {
    abstract override def onNext(t: T): Unit = {
      super.onNext(t)
      subscription.fold(())(_.cancel())
    }
  }

  "Publisher Subscriber with cancel" should {
    "do cancel" in {
      val source: Process[Task, Int] = naturals
      val latch = new CountDownLatch(1)

      ScalazProcessPublisher[Int](source.take(189))
        .subscribe(new SyncProcessSubscriber1[Int](latch, 10))

      assert(latch.await(3, TimeUnit.SECONDS))
    }
  }

  "Publisher Subscriber with OnNextBlowupSubscriber" should {
    "have thrown onNext" in {
      val sync = new SyncVar[Long]()
      val source: Process[Task, Int] = naturals
      val errors = new AtomicReference[Throwable]

      ScalazProcessPublisher[Int](source.take(25))
        .subscribe(new SyncProcessSubscriber[Int](11, sync, errors) with OnNextBlowupSubscriber[Int])

      sync.get
      errors.get().getMessage must be === "onNext"
    }
  }

  "Publisher and 3 Subscribers with different request numbers" should {
    "run" in {
      val Size = 100
      val sync = new SyncVar[Long]()
      val errors = new AtomicReference[Throwable]
      val source: Process[Task, Int] = naturals.map { r ⇒
        Thread.sleep(ThreadLocalRandom.current().nextInt(100, 150))
        r
      }

      val P = ScalazProcessPublisher[Int](source.take(Size))

      P.subscribe(new SyncProcessSubscriber[Int](26, sync, errors) with RandomRequestSubscriber[Int])
      Thread.sleep(1500)

      val sync1 = new SyncVar[Long]()
      P.subscribe(new SyncProcessSubscriber[Int](2, sync1, errors) with RandomRequestSubscriber[Int])

      Thread.sleep(1500)

      val sync2 = new SyncVar[Long]()
      P.subscribe(new SyncProcessSubscriber[Int](3, sync2, errors) with RandomRequestSubscriber[Int])

      sync1.get
      sync2.get
      sync.get must be === Size
    }
  }
}