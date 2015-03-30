package akkaScalazStreams

import akka.actor.ActorSystem
import org.scalatest.MustMatchers
import org.scalatest.WordSpecLike
import org.scalatest.BeforeAndAfterAll
import org.scalatest.BeforeAndAfterEach
import akka.testkit.{ ImplicitSender, TestKit }

import scala.collection.mutable.Buffer
import scalaz.concurrent.Task
import scalaz.stream.Process._
import scalaz.stream.io

class AkkaScalazStreamsSpec extends TestKit(ActorSystem("Integration"))
    with WordSpecLike with MustMatchers
    with BeforeAndAfterEach
    with BeforeAndAfterAll
    with ImplicitSender {

  import akka.actor.ActorDSL._
  import scalaz.stream.Process
  import scala.concurrent.duration._

  implicit val to = akka.util.Timeout(1 seconds)

  override def afterAll() {
    TestKit.shutdownActorSystem(system)
  }

  //Mutable state lives there
  def actorChannel = actor(new Act {
    private var history = List[Int]()
    become {
      case x: Int ⇒
        history = x :: history
        sender() ! Integer.toHexString(x.hashCode())
      case 'Entry =>
        sender() ! history.reverse
    }
  })

  "Scalaz-Streams api integration with akka actors" must {
    "with ask pattern" in {
      val P = Process
      val range = 0 until 15

      val ch = actorChannel

      val Source: Process[Task, Int] = for {
        p ← P.emitAll(range)
        i ← P.emit(p)
      } yield i

      val results = Buffer.empty[String]

      (Source through ch.askChannel[Int, String] observe (scalaz.stream.io.stdOutLines) to io.fillBuffer(results))
        .onFailure { ex ⇒ println("Error: " + ex.getMessage); P.halt }
        .onComplete(P.eval(Task.delay(println("Complete..."))))
        .runLog.run

      results.toList must be === range.map(s => Integer.toHexString(s.hashCode())).toList

      ch ! 'Entry
      expectMsg(range.toList)
    }
  }
}