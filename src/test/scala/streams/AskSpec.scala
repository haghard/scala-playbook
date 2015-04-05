package streams

import akka.actor._
import org.apache.log4j.Logger
import akka.testkit.{ ImplicitSender, TestKit }
import org.scalatest.{ BeforeAndAfterAll, BeforeAndAfterEach, MustMatchers, WordSpecLike }

import scala.collection.mutable.Buffer
import scala.concurrent.SyncVar
import scalaz.concurrent.Task
import scalaz.stream._

class AskSpec extends TestKit(ActorSystem("Integration"))
    with WordSpecLike with MustMatchers
    with BeforeAndAfterEach
    with BeforeAndAfterAll
    with ImplicitSender {

  private val logger = Logger.getLogger("integration")

  import akka.actor.ActorDSL._

  import scala.concurrent.duration._
  import scalaz.stream.Process

  implicit val to = akka.util.Timeout(1 seconds)

  override def afterAll() {
    TestKit.shutdownActorSystem(system)
  }

  val P = Process

  //Mutable state lives there
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

  "Scalaz-Streams integration with akka actors" must {
    "with ask pattern" in {
      val range = 0 until 15

      val ch = actorChannel

      val Source: Process[Task, Int] = for {
        p ← P.emitAll(range)
        i ← P.emit(p)
      } yield i

      val results = Buffer.empty[String]

      (Source through ch.akkaChannel[Int, String]() observe (scalaz.stream.io.stdOutLines) to io.fillBuffer(results))
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