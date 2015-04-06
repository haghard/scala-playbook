package streams.api

import java.util.concurrent.Executors._

import mongo.MongoProgram.NamedThreadFactory
import org.specs2.mutable.Specification

import scala.concurrent.SyncVar
import scalaz.concurrent.Task
import scalaz.stream.{ Process, Cause }

class ApiSpec extends Specification {

  val range = 1 to 2561

  "Publisher Subscriber start point implementation" should {
    "run" in {
      val s = 14
      val sync = new SyncVar[Boolean]()
      implicit val ex = newFixedThreadPool(4, new NamedThreadFactory("pub-sub"))

      val source: Process[Task, Int] = Process.emitAll(range)

      new ProcessPublisher[Int](source, s)
        .subscribe(new ProcessSubscriber[Int](s, sync))

      sync.get should be equalTo true
    }
  }
}
