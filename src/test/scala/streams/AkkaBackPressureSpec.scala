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
    .withInputBuffer(8, 16)
    .withDispatcher("akka.flow-dispatcher"))

  def shift(offset: Int) = Flow() { implicit b ⇒
    import FlowGraph.Implicits._
    val broadcast = b.add(Broadcast[Int](2))
    val zip = b.add(Zip[Int, Int])
    val processing = b.add(Flow[(Int, Int)] map { nums ⇒ logger.debug(s"handle: $nums"); (nums._1, nums._2) })
    broadcast ~> Flow[Int].buffer(offset, akka.stream.OverflowStrategy.backpressure) ~> zip.in0
    broadcast ~> Flow[Int].drop(offset) ~> zip.in1
    zip.out ~> processing

    (broadcast.in, processing.outlet)
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

      val sink = Sink.fold(List[String]()) { (acc: List[String], pair: (Int, Int)) ⇒
        logger.debug(s"Fold: $pair")
        acc :+ pair._1 + "-" + pair._2
      }

      val future = (Source(Limit to 0 by -2) via shift(offset)).runWith(sink)

      whenReady(future) { list ⇒
        println(list)
        list.size mustBe (Limit - (offset * 2)) / 2 + 1
      }
    }
  }

  /*
  import akka.stream._
  import scala.collection._

  case class PriorityWorkerPoolShape[In, Out](jobsIn: Inlet[In], priorityJobsIn: Inlet[In],
                                              resultsOut: Outlet[Out]) extends Shape {
    override val inlets: immutable.Seq[Inlet[_]] =
      jobsIn :: priorityJobsIn :: Nil

    override val outlets: immutable.Seq[Outlet[_]] = resultsOut :: Nil

    override def deepCopy() = PriorityWorkerPoolShape(jobsIn.carbonCopy(),
      priorityJobsIn.carbonCopy(), resultsOut.carbonCopy())

    // A Shape must also be able to create itself from existing ports
    override def copyFromPorts(inlets: immutable.Seq[Inlet[_]], outlets: immutable.Seq[Outlet[_]]) = {
      assert(inlets.size == this.inlets.size)
      assert(outlets.size == this.outlets.size)
      // This is why order matters when overriding inlets and outlets
      PriorityWorkerPoolShape(inlets(0), inlets(1), outlets(0))
    }
  }

  object PriorityWorkerPool {
    def apply[In, Out](worker: Flow[In, Out, Any], workerCount: Int): Graph[PriorityWorkerPoolShape[In, Out], Unit] = {
      FlowGraph.partial() { implicit b ⇒
        import FlowGraph.Implicits._
        val priorityMerge = b.add(MergePreferred[In](1))
        val balance = b.add(Balance[In](workerCount))
        val resultsMerge = b.add(Merge[Out](workerCount))
        // After merging priority and ordinary jobs, we feed them to the balancer
        priorityMerge ~> balance

        // Wire up each of the outputs of the balancer to a worker flow
        // then merge them back
        for (i ← 0 until workerCount)
          balance.out(i) ~> worker ~> resultsMerge.in(i)
        // We now expose the input ports of the priorityMerge and the output
        // of the resultsMerge as our PriorityWorkerPool ports
        // -- all neatly wrapped in our domain specific Shape
        PriorityWorkerPoolShape(
          jobsIn = priorityMerge.in(0),
          priorityJobsIn = priorityMerge.preferred,
          resultsOut = resultsMerge.out)
      }
    }
  }

  "PriorityWorkerPool" should {
    "run" in {
      val worker1 = Flow[String].map("step 1 " + _)
      val worker2 = Flow[String].map("step 2 " + _)

      FlowGraph.closed() { implicit b ⇒
        import FlowGraph.Implicits._
        val priorityPool1 = b.add(PriorityWorkerPool(worker1, 4))
        val priorityPool2 = b.add(PriorityWorkerPool(worker2, 2))

        Source(1 to 100).map("job: " + _) ~> priorityPool1.jobsIn
        Source(1 to 100).map("priority job: " + _) ~> priorityPool1.priorityJobsIn

        priorityPool1.resultsOut ~> priorityPool2.jobsIn

        Source(1 to 100).map("one-step, priority " + _) ~> priorityPool2.priorityJobsIn
        priorityPool2.resultsOut ~> Sink.foreach(println)
      }.run()
    }
  }*/
}