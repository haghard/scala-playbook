package streams.intergation

import akka.actor.{ ActorRef, ActorSystem }
import akka.stream.actor.{ ActorSubscriber, ActorPublisher }
import akka.stream.scaladsl._
import akka.stream.{ ActorFlowMaterializer, ActorFlowMaterializerSettings }
import akka.testkit.{ ImplicitSender, TestKit }
import org.scalatest.{ BeforeAndAfterAll, BeforeAndAfterEach, MustMatchers, WordSpecLike }

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ SyncVar, ExecutionContext }
import scala.util.{ Success, Failure }
import scalaz.concurrent.Task
import scalaz.stream._

class ApiIntegrationSpec extends TestKit(ActorSystem("integration"))
    with WordSpecLike with MustMatchers
    with BeforeAndAfterEach with BeforeAndAfterAll
    with ImplicitSender {

  override def afterAll() {
    TestKit.shutdownActorSystem(system)
  }

  object AkkaContext {
    implicit val executionContext: ExecutionContext = system.dispatcher
    implicit val materializer = ActorFlowMaterializer(ActorFlowMaterializerSettings(system))
  }

  val P = Process

  import AkkaContext._
  import streams._

  val limit = 231
  val range = 1 to limit

  def throttle[T](rate: FiniteDuration): Flow[T, T, Unit] = {
    Flow() { implicit builder ⇒
      import akka.stream.scaladsl.FlowGraph.Implicits._
      val zip = builder.add(Zip[T, Unit.type]())
      Source(rate, rate, Unit) ~> zip.in1
      (zip.in0, zip.out)
    }.map(_._1)
  }

  "Scalaz-Streams toAkkaFlow" must {
    "run" in {
      import scalaz._
      import Scalaz._

      //count ack messages
      implicit val M = Monoid[Int]
      val source: Process[Task, Int] = P.emitAll(range)

      source.toAkkaFlow
        .fold1Map(_ ⇒ 1)
        .runLog.run must be === Vector(range.size)
    }
  }

  "Scalaz-Stream process through ActorPublisher ActorSubscriber one by one" must {
    "run" in {
      //SequentualSinkSubscriber extention point for flow
      val source: Process[Task, Int] = P.emitAll(range)
      source.throughAkkaFlow
        .runLog.run must be === range.toVector
    }
  }

  /**
   * Zip 2 scalaz-stream processes with akka Zip
   * +-------------+
   * |Process(odd) |---+
   * +-------------+   | +---------------+
   *                   +-|Zip(odd, even) |
   * +-------------+   | +---------------+
   * |Process(even)|---+
   * +-------------+
   */
  "Scalaz-Stream processes to Akka Source" must {
    "run" in {

      def zip2Source[T](f: ActorRef, s: ActorRef): Source[(T, T), Unit] =
        Source() { implicit builder: FlowGraph.Builder ⇒
          import FlowGraph.Implicits._
          val zip = builder.add(Zip[T, T]())
          Source(ActorPublisher[T](f)) ~> zip.in0
          Source(ActorPublisher[T](s)) ~> zip.in1
          zip.out
        }

      val sync = new SyncVar[Boolean]
      val odd: Process[Task, Int] = P.emitAll(range).filter(_ % 2 != 0)
      val even: Process[Task, Int] = P.emitAll(range).filter(_ % 2 == 0)

      val Podd = system.actorOf(streams.BatchWriter.props[Int])
      val Peven = system.actorOf(streams.BatchWriter.props[Int])

      (odd to Podd.writer[Int]).run.runAsync(_ ⇒ ())
      (even to Peven.writer[Int]).run.runAsync(_ ⇒ ())

      val zipSrc = zip2Source(Podd, Peven) map { v ⇒ s"${v._1} - ${v._2}" }
      (zipSrc.toMat(Sink.foreach(x ⇒ println(s"read: $x")))(Keep.right)).run()
        .onComplete { _ ⇒ sync.put(true) }
      sync.get
    }
  }

  /**
   * Broadcast from scalaz-stream process to akka flow
   *                        +------------+
   *                    +---|Sink.foreach|
   * +--------------+   |   +------------+
   * |Process(range)|---+
   * +--------------+   |   +------------+
   *                    +---|Sink.foreach|
   *                        +------------+
   */
  "Scalaz-Stream process throw Akka Flow" must {
    "run" in {
      val sync = new SyncVar[Boolean]
      val digits: Process[Task, Int] = P.emitAll(range)
      val p = system.actorOf(streams.BatchWriter.props[Int])
      (digits to p.writer[Int]).run.runAsync(_ ⇒ ())

      val leftSink = Sink.foreach[Int](x ⇒ println(s"left: $x"))
      val rightSink = Sink.foreach[Int](x ⇒ println(s"right: $x"))

      val (lf, rf) = FlowGraph.closed(leftSink, rightSink)((_, _)) { implicit b ⇒
        (lout, rout) ⇒
          import FlowGraph.Implicits._
          val bcast = b.add(Broadcast[Int](2))
          Source(ActorPublisher[Int](p)) ~> bcast
          bcast.out(0) ~> lout
          bcast.out(1) ~> rout
      }.run()

      lf zip rf onComplete {
        case Success(r)  ⇒ sync.put(true)
        case Failure(ex) ⇒ throw ex
      }
      sync.get must be === true
    }
  }

  /**
   * Broadcast from akka to scalaz-stream
   *                              +------------------+
   *                         +----| Sink.foreach     |
   *                         |    +------------------+
   * +-------------------+   |    +------------------+
   * |(Akka)Source(range)|---+----| Process(subEven) |
   * +-------------------+   |    +------------------+
   *                         |    +------------------+
   *                         +----| Process(subPrime)|
   *                              +------------------+
   */
  "Akka Flow to Scalaz-Stream process" must {
    "run" in {
      val sync0 = new SyncVar[Boolean]()
      val sync1 = new SyncVar[Boolean]()

      def isPrime(n: Int) = {
        def primes = sieve(from(2))
        def from(n: Int): Stream[Int] = n #:: from(n + 1)
        def sieve(s: Stream[Int]): Stream[Int] =
          s.head #:: sieve(s.tail filter (_ % s.head != 0))

        lazy val primesN = primes.take(limit)
        primesN.contains(n)
      }

      val subEven = system.actorOf(Reader.props[Int], "even-reader")
      val subPrime = system.actorOf(Reader.props[Int], "prime-reader")

      val odd = akka.stream.scaladsl.Sink.foreach[Int](v ⇒ println(s"odd $v"))
      val even = akka.stream.scaladsl.Sink(ActorSubscriber[Int](subEven))
      val prime = akka.stream.scaladsl.Sink(ActorSubscriber[Int](subPrime))

      val src = akka.stream.scaladsl.Source(range)

      FlowGraph.closed() { implicit b ⇒
        import FlowGraph.Implicits._
        val bcast = b.add(Broadcast[Int](3))
        src ~> bcast.in
        bcast.out(0) ~> Flow[Int].filter { _ % 2 != 0 } ~> odd
        bcast.out(1) ~> Flow[Int].filter { _ % 2 == 0 } ~> even
        bcast.out(2) ~> Flow[Int].filter { isPrime } ~> prime
      }.run

      subEven.reader[Int].map { x ⇒
        println(s"even $x")
        x
      }.take(limit / 2).run.runAsync(_ ⇒ sync0.put(true))

      subPrime.reader[Int].map { x ⇒
        println(s"prime $x")
        x
      }.take(25).run.runAsync(_ ⇒ sync1.put(true))

      sync1.get
      sync0.get must be === true
    }
  }

  "Scalaz-Stream process through ActorPublisher ActorSubscriber with batching" must {
    "run" in {
      val size = 16 //
      val source: Process[Task, Int] = P.emitAll(range)
      val flow = Flow[Int].map(_ * 2)

      implicit val mat = ActorFlowMaterializer(
        ActorFlowMaterializerSettings(system)
          .withInputBuffer(initialSize = size * 2, maxSize = size * 2))

      (source.throughBufferedAkkaFlow(size, flow)(system, mat))
        .fold1(_ ++ _)
        .runLog.run must be === Vector(range.toVector.map(_ * 2))
    }
  }
}