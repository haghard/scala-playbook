package streams

import java.util.concurrent.{TimeUnit, CountDownLatch}
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
import scala.language.higherKinds

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
  def akkaActor = actor(new Act {
    private var history = List[Int]()
    become {
      case x: Int ⇒
        history = x :: history
        sender() ! Integer.toHexString(x.hashCode())
      case 'Entry ⇒
        sender() ! history.reverse
    }
  })

  sealed trait Domain[In,Out] {
    def in: In
    def out: Out
  }
  case class DomainString[In](val in: In, val out: String) extends Domain[In, String]

  sealed trait Protocol {
    type In
    type Out
    def value: In
    def cb: (Throwable \/ Out ⇒ Unit)
  }

  def envelop[T, B, C <: Protocol](v: T, f: Throwable \/ B ⇒ Unit) = new Protocol {
    override type In = T
    override type Out = B
    override def value: In = v
    override def cb: Throwable \/ Out ⇒ Unit = f
  }

  import scalaz.concurrent.{ Actor ⇒ ActorZ }
  val S = newFixedThreadPool(3, new NamedThreadFactory("scalaz-actor"))

  def scalazActor() = ActorZ[Protocol] {
    case p: Protocol ⇒
      p.cb(
        \/.right(
          DomainString(p.value, s"${Thread.currentThread().getName}: Hello from type message ${p.value.getClass.getName}")
            .asInstanceOf[p.Out]
        )
      )

    case other ⇒ ()
  }(Strategy.Executor(S))

  implicit class ScalazActorSyntax[A <: Protocol](val self: scalaz.concurrent.Actor[A]) {

    def scalazChannel[F[_], T]: Channel[Task, T, F[T]] = {
      scalaz.stream.channel.lift[Task, T, F[T]] { message: T ⇒
        Task.async { cb: (\/[Throwable, F[T]] ⇒ Unit) ⇒ self ! envelop(message, cb).asInstanceOf[A] }
      }
    }
  }

  "Process through scalaz actors" must {
    "request/response" in {
      val latch = new CountDownLatch(2)
      val source: Process[Task, Double] = P.emitAll(Seq(0.1, 0.67, 0.35, 0.68, 0.11, 8, 98.89, 9, 3.4, 67.89, 5.12, 90.45))
      val source2: Process[Task, Int] = P.emitAll(Seq.range(1, 11))

      Task.fork {
        (source through scalazActor().scalazChannel[DomainString, Double]).map(_.toString)
          .to(scalaz.stream.io.stdOutLines).run[Task]
      }(S).runAsync(_ ⇒ latch.countDown())

      Task.fork {
        (source2 through scalazActor().scalazChannel[DomainString, Int]).map(_.toString)
          .to(scalaz.stream.io.stdOutLines).run[Task]
      }(S).runAsync(_ ⇒ latch.countDown())

      latch.await(5, TimeUnit.SECONDS) must be === true
    }
  }

  "Scalaz-Streams integration with akka actors" must {
    "with ask pattern" in {
      val range = 0 until 15

      val actorRef = akkaActor

      val Source: Process[Task, Int] = for {
        p ← P.emitAll(range)
        i ← P.emit(p)
      } yield i

      val results = mutable.Buffer.empty[String]

      (Source through actorRef.akkaChannel[Int, String]() observe scalaz.stream.io.stdOutLines to scalaz.stream.io.fillBuffer(results))
        .onFailure { ex ⇒ println("Error: " + ex.getMessage); P.halt }
        .onComplete(P.eval(Task.delay(println("Complete..."))))
        .runLog.run

      results.toList must be === range.map(s ⇒ Integer.toHexString(s.hashCode())).toList

      actorRef ! 'Entry
      expectMsg(range.toList)
    }
  }

  "Scalaz-Streams integration with akka actors" must {
    "wrong out type" in {
      val ch = akkaActor
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