package streams

import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors._

import akka.actor._
import mongo.MongoProgram.NamedThreadFactory
import akka.testkit.{ ImplicitSender, TestKit }
import org.scalatest.{ BeforeAndAfterAll, BeforeAndAfterEach, MustMatchers, WordSpecLike }

import scala.collection.mutable

import scala.concurrent.SyncVar
import scalaz.stream.Channel
import scalaz.{ \/-, \/ }
import scalaz.concurrent.{ Strategy, Task }

class ActorStreamIntegrationSpec extends TestKit(ActorSystem("Integration"))
    with WordSpecLike with MustMatchers
    with BeforeAndAfterEach
    with BeforeAndAfterAll
    with ImplicitSender {

  import akka.actor.ActorDSL._
  import scala.concurrent.duration._
  import scalaz.stream.Process

  implicit val to = akka.util.Timeout(1 seconds)

  override def afterAll() {
    TestKit.shutdownActorSystem(system)
  }

  val P = Process

  //Mutable state lives here
  def actorChannel = actor(new Act {
    private var history = List[Int]()
    become {
      case x: Int ⇒
        history = x :: history
        sender() ! Integer.toHexString(x.hashCode())
      case 'Entry ⇒
        sender() ! history.reverse
    }
  })

  sealed trait Response
  case class GenericResponse[T](in: T, out: String) extends Response

  sealed trait Request {
    type In
    type Out
    def value: In
    def cb: (Throwable \/ Out ⇒ Unit)
  }

  def envelop[T, B, C <: Request](v: T, f: Throwable \/ B ⇒ Unit) = new Request {
    override type In = T
    override type Out = B
    override def value: In = v
    override def cb: Throwable \/ Out ⇒ Unit = f
  }

  import scalaz.concurrent.{ Actor ⇒ ActorZ }
  val S = newFixedThreadPool(3, new NamedThreadFactory("actors"))

  def scalazActor = ActorZ[Request] {
    case e: Request ⇒ {
      e.cb(\/-(GenericResponse(e.value,
        s"${Thread.currentThread().getName}: Hello from type message ${e.value.getClass.getName}").asInstanceOf[e.Out]))
    }
    case other ⇒ ()
  }(Strategy.Executor(S))

  implicit class ScalazActorRefSyntax[A <: Request](val self: scalaz.concurrent.Actor[A]) {
    def scalazChannel[T, B]: Channel[Task, T, B] = {
      scalaz.stream.channel.lift[Task, T, B] { message: T ⇒
        Task.async { cb: (\/[Throwable, B] ⇒ Unit) ⇒ self ! envelop(message, cb).asInstanceOf[A] }
      }
    }
  }

  "Process through scalaz actors" must {
    "request/response" in {
      val latch = new CountDownLatch(2)
      val source: Process[Task, Double] = P.emitAll(Seq(0.1, 0.67, 0.35, 0.68, 0.11, 8, 98.89, 9, 3.4, 67.89, 5.12, 90.45))
      val source2: Process[Task, Int] = P.emitAll(Seq.range(1, 11))

      Task.fork {
        (source through scalazActor.scalazChannel[Double, Response]).map(_.toString)
          .to(scalaz.stream.io.stdOutLines).runLog
      }(S).runAsync(_ ⇒ latch.countDown())

      Task.fork {
        (source2 through scalazActor.scalazChannel[Int, Response]).map(_.toString)
          .to(scalaz.stream.io.stdOutLines).runLog
      }(S).runAsync(_ ⇒ latch.countDown())

      latch.await()
      1 === 1
    }
  }

  "Scalaz-Streams integration with akka actors" must {
    "with ask pattern" in {
      val range = 0 until 15

      val ch = actorChannel

      val Source: Process[Task, Int] = for {
        p ← P.emitAll(range)
        i ← P.emit(p)
      } yield i

      val results = mutable.Buffer.empty[String]

      (Source through ch.akkaChannel[Int, String]() observe scalaz.stream.io.stdOutLines to scalaz.stream.io.fillBuffer(results))
        .onFailure { ex ⇒ println("Error: " + ex.getMessage); P.halt }
        .onComplete(P.eval(Task.delay(println("Complete..."))))
        .runLog.run

      results.toList must be === range.map(s ⇒ Integer.toHexString(s.hashCode())).toList

      ch ! 'Entry
      expectMsg(range.toList)
    }
  }

  "Scalaz-Streams integration with akka actors" must {
    "wrong out type" in {
      val ch = actorChannel
      val range = 0 until 2
      val r = new SyncVar[String]

      val Source: Process[Task, Int] = for {
        p ← P.emitAll(range)
        i ← P.emit(p)
      } yield i

      (Source through ch.akkaChannel[Int, Float]())
        .onFailure { ex ⇒ r.put(ex.getMessage); P.halt }
        .onComplete(P.eval(Task.delay(println("Complete..."))))
        .run.run

      r.get(1000) must be === Some("Cannot cast java.lang.String to java.lang.Float")
    }
  }
}