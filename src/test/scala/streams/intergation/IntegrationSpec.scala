package streams.intergation

import akka.stream.scaladsl._
import akka.actor.{ ActorRef, ActorSystem }
import akka.testkit.{ ImplicitSender, TestKit }
import org.scalatest.concurrent.AsyncAssertions.Waiter
import akka.stream.actor.{ ActorSubscriber, ActorPublisher }
import akka.stream.{ Attributes, ActorMaterializer, ActorMaterializerSettings }
import org.scalatest.{ BeforeAndAfterAll, BeforeAndAfterEach, MustMatchers, WordSpecLike }

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.SyncVar
import scala.util.{ Try, Success, Failure }
import scalaz.concurrent.Task
import scalaz.stream._

class IntegrationSpec extends TestKit(ActorSystem("integration"))
    with WordSpecLike with MustMatchers
    with BeforeAndAfterEach with BeforeAndAfterAll
    with ImplicitSender {

  val P = Process

  override def afterAll() {
    TestKit.shutdownActorSystem(system)
  }

  object AkkaContext {
    implicit val materializer = ActorMaterializer(ActorMaterializerSettings(system)
      .withDispatcher("akka.flow-dispatcher"))
    implicit val E = streams.ExecutionContextExecutorServiceBridge(materializer.executionContext)
  }

  import AkkaContext._
  import streams._

  val limit = 152
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
      val ssSource: Process[Task, Int] = P.emitAll(range)

      ssSource.toAkkaFlow
        .fold1Map(_ ⇒ 1)
        .runLog.run must be === Vector(range.size)
    }
  }

  "Scalaz-Stream process through ActorPublisher ActorSubscriber one by one" must {
    "run" in {
      P.emitAll(range).sourceToSink.runLog.run must be === range.toVector
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
  "Scalaz-Stream processes to Akka Source(like sstream tee)" must {
    "run fan-out operation" in {
      val sync = new SyncVar[Boolean]

      //act like tee, deterministically merge
      def tee[T](left: ActorRef, right: ActorRef): Source[(T, T), Unit] =
        Source() { implicit builder ⇒
          import FlowGraph.Implicits._
          val zip = builder.add(Zip[T, T]())
          Source(ActorPublisher[T](left)) ~> zip.in0
          Source(ActorPublisher[T](right)) ~> zip.in1
          zip.out
        }

      val odd: Process[Task, Int] = P.emitAll(range).filter(_ % 2 != 0)
      val even: Process[Task, Int] = P.emitAll(range).filter(_ % 2 == 0)

      val Podd = system.actorOf(streams.BatchWriter.props[Int])
      val Peven = system.actorOf(streams.BatchWriter.props[Int])

      (odd to Podd.writer[Int]).run.runAsync(_ ⇒ ())
      (even to Peven.writer[Int]).run.runAsync(_ ⇒ ())

      val zipper = tee[Int](Podd, Peven) map { v ⇒ s"${v._1} - ${v._2}" }
      (zipper.toMat(Sink.foreach(x ⇒ println(s"read: $x")))(Keep.right))
        .run()
        .onComplete { _ ⇒ sync.put(true) }

      sync.get must be === true
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
    "run fan-in operation" in {
      val w = new Waiter

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
        case Success(r)  ⇒ w.dismiss()
        case Failure(ex) ⇒ throw ex
      }
      w.await()
    }
  }

  /**
   * Broadcast from akka source to scalaz-stream
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
      val evenSync = new SyncVar[Boolean]()
      val primeSync = new SyncVar[Boolean]()

      def isPrime(n: Int) = {
        def primes = sieve(from(2))
        def from(n: Int): Stream[Int] = n #:: from(n + 1)
        def sieve(s: Stream[Int]): Stream[Int] =
          s.head #:: sieve(s.tail filter (_ % s.head != 0))

        lazy val primesN = primes.take(limit)
        primesN.contains(n)
      }

      val evenR = system.actorOf(Reader.props[Int], "even-reader")
      val primeR = system.actorOf(Reader.props[Int], "prime-reader")

      val odd = akka.stream.scaladsl.Sink.foreach[Int](v ⇒ println(s"odd $v"))
      val even = akka.stream.scaladsl.Sink(ActorSubscriber[Int](evenR))
      val prime = akka.stream.scaladsl.Sink(ActorSubscriber[Int](primeR))

      val src = akka.stream.scaladsl.Source(range)

      FlowGraph.closed() { implicit b ⇒
        import FlowGraph.Implicits._
        val bcast = b.add(Broadcast[Int](3))
        src ~> bcast.in
        bcast.out(0) ~> Flow[Int].filter { _ % 2 != 0 } ~> odd
        bcast.out(1) ~> Flow[Int].filter { _ % 2 == 0 } ~> even
        bcast.out(2) ~> Flow[Int].filter { isPrime } ~> prime
      }.run

      evenR.reader[Int].map { x ⇒
        println(s"even $x")
        x
      }.take(limit / 2).run.runAsync(_ ⇒ evenSync.put(true))

      primeR.reader[Int].map { x ⇒
        println(s"prime $x")
        x
      }.take(25).run.runAsync(_ ⇒ primeSync.put(true))

      primeSync.get
      evenSync.get must be === true
    }
  }

  /**
   *
   * +---------------+    +-----+   +------------------------+
   * |Process[Task,T]|----|Flow |---|Process[Task, Vector[T]]|
   * +---------------+    +-----+   +------------------------+
   *
   */
  "Scalaz-Stream process through Akka Flow with batching" must {
    "run" in {
      val size = 8 //
      val source: Process[Task, Int] = P.emitAll(range)

      //The example below will ensure that size*2 jobs (but not more) are enqueue from an external process and stored locally in memory
      val flow = Flow[Int].map(_ * 2)
        .withAttributes(Attributes.inputBuffer(initial = size * 2, max = size * 2))

      source.throughAkkaFlow(size, flow).fold1(_ ++ _)
        .runLog.run must be === Vector(range.toVector.map(_ * 2))
    }
  }

  /**
   *
   * +----------+      +----------+     +-----+   +--------+
   * |Process   |------|AkkaSource|-----|Flow |---|AkkaSink|
   * +----------+      +----------+     +-- --+   +--------+
   *
   */
  "Scalaz-Stream process to AkkaSource through Flow to AkkaSink" must {
    "run" in {
      val size = 8
      val sync = new SyncVar[Try[Int]]()

      val akkaSource = P.emitAll(range).toAkkaSource
      val akkaSink = akka.stream.scaladsl.Sink.fold[Int, Int](0)(_ + _)

      val flow = Flow[Int].map { x ⇒ println(Thread.currentThread().getName); x * 2 }
        .withAttributes(Attributes.inputBuffer(initial = size * 2, max = size * 2))

      FlowGraph.closed(akkaSource, akkaSink)((_, _)) { implicit b ⇒
        import FlowGraph.Implicits._
        (src, sink) ⇒ src ~> flow ~> sink
      }.run()._2.onComplete(r ⇒ sync.put(r))

      sync.get must be === Success(range.sum * 2)
    }
  }
}