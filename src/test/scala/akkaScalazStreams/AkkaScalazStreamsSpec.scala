package akkaScalazStreams

import akka.actor.ActorSystem
import akka.testkit.{ ImplicitSender, TestKit }
import org.scalatest.BeforeAndAfterAll
import org.scalatest.BeforeAndAfterEach
import org.scalatest.MustMatchers
import org.scalatest.WordSpecLike

import scalaz.concurrent.Task

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

  "Scalaz-Streams api integration with akka actors" must {
    "with ask" in {
      val P = Process
      val range = 0 until 10

      val Source: Process[Task, Int] = for {
        p ← P.emitAll(range)
        i ← P.emit(p)
      } yield i

      val actorChannel = actor(new Act {
        private var messages = List[Int]()
        become {
          case x: Int ⇒
            messages = x :: messages
            sender() ! Integer.toHexString(x.hashCode())
          case 'Entry ⇒ sender() ! messages.reverse
        }
      })

      (Source through actorChannel.askChannel[Int, String] observe (scalaz.stream.io.stdOutLines))
        .onFailure(ex ⇒ Process.eval(Task.delay(println("Error: " + ex.getMessage))))
        .runLog.run.size must be === 10

      actorChannel ! 'Entry
      expectMsg(range.toList)
    }
  }
}