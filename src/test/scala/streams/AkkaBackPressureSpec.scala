package streams

import akka.testkit.TestKit
import akka.stream.scaladsl._
import akka.actor.ActorSystem
import org.apache.log4j.Logger
import org.scalatest.concurrent.ScalaFutures
import akka.stream.{ ActorMaterializer, ActorMaterializerSettings }
import org.scalatest.{ BeforeAndAfterAll, BeforeAndAfterEach, MustMatchers, WordSpecLike }

class AkkaBackPressureSpec extends TestKit(ActorSystem("streams")) with WordSpecLike
    with MustMatchers with BeforeAndAfterEach
    with BeforeAndAfterAll with ScalaFutures {

  val logger = Logger.getLogger("akka-streams")

  override def afterAll() {
    TestKit.shutdownActorSystem(system)
  }

  implicit val materializer = ActorMaterializer(ActorMaterializerSettings(system)
    .withDispatcher("akka.flow-dispatcher"))

  def shift(offset: Int) = Flow() { implicit b ⇒
    import FlowGraph.Implicits._

    val bcast = b.add(Broadcast[Int](2))
    val zip = b.add(Zip[Int, Int])

    val processing = b.add(Flow[(Int, Int)] map { nums ⇒ logger.debug(s"handle: $nums"); (nums._1, nums._2) })
    bcast ~> Flow[Int].buffer(offset, akka.stream.OverflowStrategy.backpressure) ~> zip.in0
    bcast ~> Flow[Int].drop(offset) ~> zip.in1
    zip.out ~> processing

    (bcast.in, processing.outlet)
  }

  /**
   * http://blog.lancearlaus.com/akka/streams/scala/2015/05/27/Akka-Streams-Balancing-Buffer/
   *
   * Deadlock occurs due to the Drop combined with the behavior of Broadcast and Zip.
   * Broadcast won’t emit an element until all outputs (branches) signal demand while Zip won’t signal demand until
   * all its inputs (branches) have emitted an element.
   * This makes sense since Broadcast is constrained by the slowest consumer and Zip must emit well-formed tuples.
   */
  "Akka-Streams-Balancing-Buffer" should {
    "avoid deadlock that occurs due to the Drop combined with the behavior of Broadcast and Zip" in {
      val Limit = 100
      val offset = 8
      val future = Source(Limit to 0 by -2)
        .via(shift(offset))
        .runWith(Sink.fold(List[String]()) { (acc: List[String], pair: (Int, Int)) ⇒
          logger.debug(s"Fold: $pair")
          acc :+ pair._1 + "-" + pair._2
        })

      whenReady(future) { list ⇒
        println(list)
        list.size mustBe (Limit - (offset * 2)) / 2 + 1
      }
    }
  }
}