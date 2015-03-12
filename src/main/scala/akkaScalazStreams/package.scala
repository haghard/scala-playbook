import java.util.concurrent.Executors

import akka.actor.ActorRef
import akka.util.Timeout
import mongo.MongoProgram.NamedThreadFactory

import scala.concurrent.Future
import scala.reflect.ClassTag
import scala.util.{ Failure, Success }
import scalaz.{ -\/, \/- }
import scalaz.concurrent.Task
import scalaz.stream._
import akka.pattern.ask

package object akkaScalazStreams {

  private val executor = Executors.newFixedThreadPool(2, new NamedThreadFactory("io-worker"))
  implicit val ec = scala.concurrent.ExecutionContext.fromExecutor(executor)

  implicit class FutureOps[+A](f: ⇒ Future[A]) {
    def toTask: Task[A] = Task async { cb ⇒
      f.onComplete {
        case Success(v) ⇒ cb(\/-(v))
        case Failure(e) ⇒ cb(-\/(e))
      }
    }
  }

  implicit class ActorRefOps(val self: ActorRef) extends AnyVal {
    def askChannel[A, B](implicit timeout: Timeout, tag: ClassTag[B]): Channel[Task, A, B] =
      askActor[A, B](self)
  }

  def askActor[A, B](actor: ActorRef)(implicit timeout: Timeout, tag: ClassTag[B]): Channel[Task, A, B] = {
    io.channel[Task, A, B] { message: A ⇒
      Task suspend { (actor ask message).mapTo[B] toTask }
    }
  }
}
