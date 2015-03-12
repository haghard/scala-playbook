package akkaScalazStreams

import akka.actor.{ ActorLogging, ActorSystem }
import akka.testkit.{ ImplicitSender, TestKit }
import com.typesafe.config.ConfigFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.BeforeAndAfterEach
import org.scalatest.MustMatchers
import org.scalatest.WordSpecLike

import scalaz.concurrent.Task

object AkkaScalazStreamsSpec {
  def config = ConfigFactory.parseString(s"""
    akka {
      test-dispatcher {
        type = Dispatcher
        executor = "fork-join-executor"
        fork-join-executor {
          parallelism-min = 4
          parallelism-factor = 2.0
          parallelism-max = 20
        }
        throughput = 100
      }
    }
  """)
}

class AkkaScalazStreamsSpec extends TestKit(ActorSystem("Integration"))
    with WordSpecLike with MustMatchers
    with BeforeAndAfterEach
    with BeforeAndAfterAll
    with ImplicitSender {

  import akka.actor.ActorDSL._
  import scalaz.stream.Process
  import scala.concurrent.duration._

  override def afterAll() {
    TestKit.shutdownActorSystem(system)
  }

  implicit val to = akka.util.Timeout(1 seconds)

  "Ask akka actor" must {
    "integration with scalaz-streams" in {
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
            val r = Integer.toHexString(x.hashCode())
            sender() ! r
          case 'Entry ⇒
            sender() ! messages.reverse
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