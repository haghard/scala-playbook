package streams.api

import java.util.concurrent.Executors._
import org.specs2.mutable.Specification
import mongo.MongoProgram.NamedThreadFactory

import scala.concurrent.SyncVar
import scalaz.concurrent.{ Strategy, Task }
import scalaz.stream.Process._
import scalaz.stream._

class ApiSpec extends Specification {

  val range = 1 to 46

  implicit val ex = newFixedThreadPool(4, new NamedThreadFactory("pub-sub"))

  "Publisher Subscriber start point implementation" should {
    "run" in {
      val s = 15
      val sync = new SyncVar[Boolean]()
      val source: Process[Task, Int] = Process.emitAll(range)

      ProcessPublisher[Int](source)
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

      ProcessPublisher[Int](source)
        .subscribe(new ProcessSubscriber[Int](s, sync) with CancelableSubscriber[Int])

      sync.get should be equalTo true
    }
  }

  /*"chunk reader" should {
    "run" in {
      import scala.concurrent.duration._
      implicit val sch = newScheduledThreadPool(1, new NamedThreadFactory("Schedulers"))
      implicit val str = Strategy.Executor(newFixedThreadPool(2, new NamedThreadFactory("timeout-worker")))

      val sync = new SyncVar[Boolean]()

      def naturals = {
        def go(i: Int): Process[Task, Int] =
          Process.await(Task.delay(i))(i ⇒ Process.emit(i) ++ go(i + 1))
        go(0)
      }

      //val source: Process[Task, Int] = Process.emitAll(1 to 20)
      val source: Process[Task, Int] = naturals

      val tags: Process[Task, Int] = Process.emitAll(Seq(3, 2, 5, 5, 2, 10, 1))
      val chunkedSource = streams.io.chunkR(source)

      (for {
        n ← (time.awakeEvery(100 milli)(str, sch)) zip tags
        _ ← chunkedSource.chunk(n._2)
          .map(x ⇒ s"batch: ${n._2} content: ${x}").observe(scalaz.stream.io.stdOutLines)
      } yield ()).run.runAsync(_ ⇒ sync.put(true))

      sync.get(5000) should be equalTo Some(true)
    }
  }*/
}