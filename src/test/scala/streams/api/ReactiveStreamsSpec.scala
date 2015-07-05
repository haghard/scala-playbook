package streams.api

import java.util.concurrent.{ TimeUnit, CountDownLatch }
import java.util.concurrent.atomic.AtomicReference
import org.scalatest.{ WordSpecLike, MustMatchers }

import scala.concurrent.SyncVar
import scala.concurrent.forkjoin.ThreadLocalRandom
import scalaz.concurrent.Task
import scalaz.stream._

class ReactiveStreamsSpec extends WordSpecLike with MustMatchers {
  import Procesess._

  trait RandomRequestSubscriber[T] extends ProcessSubscriber[T] {
    override def updateBufferSize = ThreadLocalRandom.current().nextInt(1, 12)
  }

  trait OnNextBlowupSubscriber[T] extends ProcessSubscriber[T] {
    abstract override def onNext(t: T): Unit = {
      super.onNext(t)
      throw new Exception("onNext")
    }
  }

  trait OneElementSubscriber[T] extends ProcessSubscriber[T] {
    abstract override def onNext(t: T): Unit = {
      super.onNext(t)
      subscription.fold(())(_.cancel())
    }
  }

  "Publisher Subscriber with cancel" should {
    "do cancel" in {
      val source: Process[Task, Int] = naturals
      val latch = new CountDownLatch(1)

      ScalazProcessPublisher[Int](source, 189)
        .subscribe(new ProcessSubscriber1[Int](latch, 10))

      assert(latch.await(3, TimeUnit.SECONDS))
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

      val P = ScalazProcessPublisher[Int](source, Size)

      P.subscribe(new ProcessSubscriber[Int](12, sync, errors) with RandomRequestSubscriber[Int])
      Thread.sleep(3000)

      val sync1 = new SyncVar[Long]()
      P.subscribe(new ProcessSubscriber[Int](6, sync1, errors) with RandomRequestSubscriber[Int])

      Thread.sleep(2000)

      val sync2 = new SyncVar[Long]()
      P.subscribe(new ProcessSubscriber[Int](10, sync2, errors) with RandomRequestSubscriber[Int])

      assert(sync1.get > 50)
      assert(sync2.get > 30)
      sync.get must be === Size
    }
  }

  "Publisher Subscriber with OnNextBlowupSubscriber" should {
    "have thrown onNext" in {
      val sync = new SyncVar[Long]()
      val source: Process[Task, Int] = naturals
      val errors = new AtomicReference[Throwable]

      ScalazProcessPublisher[Int](source, 25)
        .subscribe(new ProcessSubscriber[Int](11, sync, errors) with OnNextBlowupSubscriber[Int])

      sync.get
      errors.get().getMessage must be === "onNext"
    }
  }

  /*"Empty Publisher" should {
    "complete after first request" in {
      val sync = new SyncVar[Long]()
      val errors = new AtomicReference[Throwable]
      val source: Process[Task, Int] = naturals

      ScalazProcessPublisher[Int](source, 0)
        .subscribe(new ProcessSubscriber[Int](1, sync, errors))

      sync.get must be === 0
    }
  }*/

}