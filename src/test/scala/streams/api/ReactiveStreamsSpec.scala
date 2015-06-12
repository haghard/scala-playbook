package streams.api

import java.util.concurrent.Executors._
import org.specs2.mutable.Specification
import mongo.MongoProgram.NamedThreadFactory

import scala.concurrent.SyncVar
import scala.concurrent.forkjoin.ThreadLocalRandom
import scalaz.concurrent.Task
import scalaz.stream._

class ReactiveStreamsSpec extends Specification {

  def naturals = {
    def go(i: Int): Process[Task, Int] =
      Process.await(Task.delay(i))(i ⇒ Process.emit(i) ++ go(i + 1))
    go(0)
  }

  implicit val ex = newFixedThreadPool(4, new NamedThreadFactory("pub-sub"))

  "Publisher Subscriber start point implementation" should {
    "run" in {
      val s = 15
      val sync = new SyncVar[Boolean]()
      val source: Process[Task, Int] = naturals.take(81)

      ScalazProcessPublisher[Int](source)
        .subscribe(new ProcessSubscriber[Int](s, sync))

      sync.get should be equalTo true
    }
  }

  trait CancelableSubscriber[T] extends ProcessSubscriber[T] {
    var i = 0

    abstract override def onNext(t: T): Unit = {
      super.onNext(t)
      i += 1
      if (i == 100) {
        subscription.fold(())(_.cancel())
      }
    }
  }

  trait RandomRequestSubscriber[T] extends ProcessSubscriber[T] {
    override val updateBufferSize = ThreadLocalRandom.current().nextInt(12, 27)
  }

  "Publisher Subscriber with cancel" should {
    "do cancel" in {
      val s = 12
      val sync = new SyncVar[Boolean]()
      val source: Process[Task, Int] = naturals.take(89)

      ScalazProcessPublisher[Int](source)
        .subscribe(new ProcessSubscriber[Int](s, sync) with CancelableSubscriber[Int])

      sync.get should be equalTo true
    }
  }

  "Publisher Subscriber with different request numbers" should {
    "run" in {
      val sync = new SyncVar[Boolean]()
      val source: Process[Task, Int] = naturals.take(25146)

      ScalazProcessPublisher[Int](source)
        .subscribe(new ProcessSubscriber[Int](11, sync) with RandomRequestSubscriber[Int])

      sync.get should be equalTo true
    }
  }
}