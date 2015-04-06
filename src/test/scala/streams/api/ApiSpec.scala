package streams.api

import java.util.concurrent.Executors._

import org.specs2.mutable.Specification
import mongo.MongoProgram.NamedThreadFactory

import scala.concurrent.SyncVar
import scalaz.concurrent.Task
import scalaz.stream.Process

class ApiSpec extends Specification {

  val range = 1 to 245

  implicit val ex = newFixedThreadPool(4, new NamedThreadFactory("pub-sub"))

  "Publisher Subscriber start point implementation" should {
    "run" in {
      val s = 15
      val sync = new SyncVar[Boolean]()
      val source: Process[Task, Int] = Process.emitAll(range)

      new ProcessPublisher[Int](source, s)
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

  "Publisher Subscriber with cancel" should {
    "do cancel" in {
      val s = 12
      val sync = new SyncVar[Boolean]()
      val source: Process[Task, Int] = Process.emitAll(range)

      new ProcessPublisher[Int](source, s)
        .subscribe(new ProcessSubscriber[Int](s, sync) with CancelableSubscriber[Int])

      sync.get should be equalTo true
    }
  }
}
